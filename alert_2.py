import telegram
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
from datetime import date, timedelta, datetime
import requests
import pandahouse

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

sns.set()

default_args = {
    'owner': 'e-terskij-10',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 6)
}

schedule_interval = '0 11 * * *'

connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220820',
                      'user':'student',
                      'password':'password'
                    }

feed_only_query = """
    SELECT toStartOfDay(toDateTime(date_dt)) AS day,
                           count(DISTINCT uid) AS dau
    FROM
    (SELECT toDate(f.time) date_dt,
            f.user_id as uid,
            m.user_id
            FROM simulator_20220820.feed_actions f
            LEFT JOIN simulator_20220820.message_actions m ON f.user_id = m.user_id
                               AND toDate(f.time) = toDate(m.time)
            WHERE m.user_id = 0
    ORDER BY toDate(f.time) DESC) AS t
    WHERE toDate(date_dt) between today() - 8 and today() - 1
    GROUP BY day
    ORDER BY day DESC
"""

dau_combined_query = """
    SELECT toStartOfDay(toDateTime(date_dt)) AS day,
            count(DISTINCT fa_user_id) AS dau
            FROM
            (SELECT toDate(f.time) date_dt,
                    f.user_id AS fa_user_id,
                    m.user_id AS ma_user_id
            FROM simulator_20220820.feed_actions f
            INNER JOIN simulator_20220820.message_actions m ON f.user_id = m.user_id
                AND toDate(f.time) = toDate(m.time)
    ORDER BY toDate(f.time) DESC) AS t
    WHERE toDate(date_dt) between today() - 8 and today() - 1
    GROUP BY day
    ORDER BY day DESC
"""

msg_per_user_query = """
    SELECT toStartOfDay(toDateTime(time)) AS day,
            round(count(user_id) / count(DISTINCT user_id), 1) AS messages
    FROM simulator_20220820.message_actions
    WHERE toDate(time) between today() - 8 and today() - 1
    GROUP BY day
    ORDER BY day DESC
"""

views_per_user_query = """
    SELECT toStartOfDay(toDateTime(time)) as day,
                round(countIf(user_id, action='view') / count(DISTINCT user_id), 1) AS views
    FROM simulator_20220820.feed_actions
    WHERE toDate(time) between today() - 8 and today() - 1
    GROUP BY day
    ORDER BY day DESC
"""

likes_per_user_query = """
    SELECT toStartOfDay(toDateTime(time)) as day,
        round(countIf(user_id, action='like') / count(DISTINCT user_id), 1) AS likes
    FROM simulator_20220820.feed_actions
    WHERE toDate(time) between today() - 8 and today() - 1
    GROUP BY day
    ORDER BY day DESC
"""

ctr_query = """
    SELECT toStartOfDay(toDateTime(time)) as day,
        round(countIf(action = 'like') / countIf(action = 'view'), 2) as ctr
            FROM simulator_20220820.feed_actions
    WHERE toDate(time) between today() - 8 and today() - 1
    GROUP BY day
    ORDER BY day DESC
"""

tg_token = '5594784718:AAGAin4gluFzJ7FmJmAhEHCMZ0VCOKDR3Rg'
my_bot = telegram.Bot(token=tg_token)
chat_id = -555114317

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def tersky_alert_2():

    @task()
    def report_application():

        feed_only = pandahouse.read_clickhouse(feed_only_query, connection=connection)
        
        dau_combined = pandahouse.read_clickhouse(dau_combined_query, connection=connection)        
         
        msg_per_user = pandahouse.read_clickhouse(msg_per_user_query, connection=connection)
              
        views_per_user = pandahouse.read_clickhouse(views_per_user_query, connection=connection)
               
        likes_per_user = pandahouse.read_clickhouse(likes_per_user_query, connection=connection)

        ctr = pandahouse.read_clickhouse(ctr_query, connection=connection)
        # представим, что интересующей бизнес метрикой является количество пользователей,
        # которые пользуются обоими сервисами одновременно, поэтому включаем эту информацию в начале отчета
        message = "Статистика по приложению на " + str(date.today()) + ':\n'\
              + '- использовали только ленту' + '\n' \
              + 'вчера: ' + str(feed_only.iloc[1, 1]) + '\n' \
              + 'неделю назад: ' + str(feed_only.iloc[7, 1]) + '\n' \
              + '- использовали ленту и сообщения' + '\n'\
              + 'вчера: ' + str(dau_combined.iloc[1, 1]) + '\n' \
              + 'неделю назад: ' + str(dau_combined.iloc[7, 1]) + '\n' \
              + '- среднее кол-во сообщений на пользователя' + '\n' \
              + 'вчера: ' + str(msg_per_user.iloc[1, 1]) + '\n' \
              + 'неделю назад: ' + str(msg_per_user.iloc[7, 1]) + '\n' \
              + '- среднее кол-во просмотров на пользователя:' + '\n' \
              + 'вчера: ' + str(views_per_user.iloc[1, 1]) + '\n' \
              + 'неделю назад: ' + str(views_per_user.iloc[7, 1]) + '\n' \
              + '- среднее кол-во лайков на пользователя:' + '\n' \
              + 'вчера: ' + str(likes_per_user.iloc[1, 1]) + '\n' \
              + 'неделю назад: ' + str(likes_per_user.iloc[7, 1])  + '\n' \
              + '- CTR: ' + '\n' \
              + 'вчера: ' + str(ctr.iloc[1, 1]) + '\n' \
              + 'неделю назад: ' + str(ctr.iloc[7, 1])

        my_bot.sendMessage(chat_id=chat_id, text=message)
      
        fig, axes = plt.subplots(3, 2, figsize=(14, 17))
        
        fig.suptitle("Статистика по приложению", fontsize=15)
        fig.subplots_adjust(hspace=0.7)
        fig.subplots_adjust(wspace=0.4)        

        sns.lineplot(data=feed_only, x="day", y="dau", ax=axes[0, 0])
        axes[0, 0].set_title('использовали только ленту', fontsize=10)
        axes[0, 0].tick_params(axis="x", rotation=45)

        sns.lineplot(data=dau_combined, x="day", y="dau", ax=axes[0, 1])
        axes[0, 1].set_title("использовали ленту и сообщения", fontsize=10)
        axes[0, 1].tick_params(axis="x", rotation=45)

        sns.lineplot(data=msg_per_user, x="day", y="messages", ax=axes[1, 0])
        axes[1, 0].set_title("сообщения на пользователя", fontsize=10)
        axes[1, 0].tick_params(axis='x', rotation=45)
                
        sns.lineplot(data=views_per_user, x='day', y="views", ax=axes[1, 1])
        axes[1, 1].set_title("просмотры на пользователя", fontsize=10)
        axes[1, 1].tick_params(axis='x', rotation=45)
        
        sns.lineplot(data=likes_per_user, x='day', y="likes", ax=axes[2, 0])
        axes[2, 0].set_title("лайки на пользователя", fontsize=10)
        axes[2, 0].tick_params(axis='x', rotation=45)

        sns.lineplot(data=ctr, x='day', y="ctr", ax=axes[2, 1])
        axes[2, 1].set_title("CTR", fontsize=10)
        axes[2, 1].tick_params(axis='x', rotation=45)

        
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = ('stats_1_7_days.png')
        plt.close()

        my_bot.sendPhoto(chat_id=chat_id, photo=plot_object)


    report_application = report_application()




tersky_alert_2 = tersky_alert_2()
