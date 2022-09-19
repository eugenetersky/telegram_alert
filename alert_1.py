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

dau_query = """
                SELECT COUNT (DISTINCT user_id)
                  FROM simulator_20220820.feed_actions
                 WHERE toDate(time) = today() - 1
"""

views_query = """
            SELECT countIf(action = 'view')
                FROM simulator_20220820.feed_actions
            WHERE toDate(time) = today() - 1
"""

likes_query = """
                SELECT countIf(action = 'like')
                    FROM simulator_20220820.feed_actions
                WHERE toDate(time) = today() - 1
"""

ctr_query = """
                SELECT countIf(action = 'like') / countIf(action = 'view')
                    FROM simulator_20220820.feed_actions
                WHERE toDate(time) = today() - 1
"""

metrics_query = """
                SELECT toDate(time) as day,
                    COUNT (DISTINCT user_id) as DAU,
                    countIf(user_id, action = 'view') as views,
                    countIf(user_id, action = 'like') as likes,
                    countIf(user_id, action = 'like') / countIf(user_id, action = 'view') as CTR
                FROM simulator_20220820.feed_actions
                WHERE toDate(time) BETWEEN today() - 8 AND today() - 1
                GROUP BY day
"""

tg_token = '5594784718:AAGAin4gluFzJ7FmJmAhEHCMZ0VCOKDR3Rg'
my_bot = telegram.Bot(token=tg_token)
chat_id = -555114317

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)

def tersky_alert_1():

    @task()
    def alert_message():

        dau = pandahouse.read_clickhouse(dau_query, connection=connection)

        views = pandahouse.read_clickhouse(views_query, connection=connection)

        likes = pandahouse.read_clickhouse(likes_query, connection=connection)

        ctr = pandahouse.read_clickhouse(ctr_query, connection=connection)

        msg = "Метрики за вчера " + str(date.today() - timedelta(days=1)) + ':' + '\n'\
              + '- уникальные пользователи: ' + str(dau.iloc[0, 0]) + '\n' \
              + '- просмотры: ' + str(views.iloc[0, 0]) + '\n' \
              + '- лайки: ' + str(likes.iloc[0, 0]) + '\n' \
              + '- CTR: ' + str(round(ctr.iloc[0, 0], 2))

        my_bot.sendMessage(chat_id=chat_id, text=msg)

    @task()
    def alert_plot():

        metrics = pandahouse.read_clickhouse(metrics_query, connection=connection)

        day = metrics['day'].dt.strftime("%d")

        fig, axes = plt.subplots(2, 2, figsize=(14, 7))
        fig.suptitle("Метрики за 7 дней")
        fig.subplots_adjust(hspace=0.7)
        fig.subplots_adjust(wspace=0.4)

        sns.lineplot(data=metrics, x=day, y='DAU', ax=axes[0, 0])
        axes[0, 0].set_title("уникальные пользователи")

        sns.lineplot(data=metrics, x=day, y='views', ax=axes[1, 1])
        axes[1, 1].set_title("просмотры")

        sns.lineplot(data=metrics, x=day, y='likes', ax=axes[0, 1])
        axes[0, 1].set_title("лайки")

        sns.lineplot(data=metrics, x=day, y='CTR', ax=axes[1, 0])
        axes[1, 0].set_title("CTR")

        plot_object = io.BytesIO()
        plt.savefig(plot_object)

        plot_object.seek(0)
        plot_object.name = ('metrics.png')
        plt.close()

        my_bot.sendPhoto(chat_id=chat_id, photo=plot_object)


    alert_message = alert_message()
    alert_plot = alert_plot()



tersky_alert_1 = tersky_alert_1()
