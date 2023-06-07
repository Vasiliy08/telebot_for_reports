import os

import telebot
from telebot import types
from __token import TOKEN
import pandas as pd
import psycopg2
from pandas import json_normalize
import json
from datetime import date
from telegram_bot_calendar import DetailedTelegramCalendar, LSTEP


def main(bot, cur):

    @bot.message_handler(commands=['cal'])
    def start1(m):
        # do not forget to put calendar_id
        calendar, step = DetailedTelegramCalendar(calendar_id=1, locale='ru').build()
        bot.send_message(m.chat.id,
                         f"Calendar 1: Select {LSTEP[step]}",
                         reply_markup=calendar)

    @bot.message_handler(commands=['cal2'])
    def start2(m):
        # do not forget to put calendar_id
        calendar, step = DetailedTelegramCalendar(calendar_id=2, locale='ru').build()
        bot.send_message(m.chat.id,
                         f"Calendar 2: Select {LSTEP[step]}",
                         reply_markup=calendar)

    @bot.callback_query_handler(func=DetailedTelegramCalendar.func(calendar_id=1))
    def cal1(c):
        # calendar_id is used here too, since the new keyboard is made
        result, key, step = DetailedTelegramCalendar(calendar_id=1).process(c.data)
        if not result and key:
            bot.edit_message_text(f"Calendar 1: Select {LSTEP[step]}",
                                  c.message.chat.id,
                                  c.message.message_id,
                                  reply_markup=key)
        elif result:
            bot.edit_message_text(f"You selected {result} in calendar 1",
                                  c.message.chat.id,
                                  c.message.message_id)

    @bot.callback_query_handler(func=DetailedTelegramCalendar.func(calendar_id=2))
    def cal1(c):
        # calendar_id is used here too, since the new keyboard is made
        result, key, step = DetailedTelegramCalendar(calendar_id=2, locale='ru').process(c.data)
        if not result and key:
            bot.edit_message_text(f"Calendar 2: Select {LSTEP[step]}",
                                  c.message.chat.id,
                                  c.message.message_id,
                                  reply_markup=key)
        elif result:
            bot.edit_message_text(f"You selected {result} in calendar 2",
                                  c.message.chat.id,
                                  c.message.message_id)


    @bot.message_handler(commands=['start'])
    def list_project(message):

        cur.execute("""
            SELECT DISTINCT partnername
            FROM mv_outcoming_call_project mocp
        """)
        partners = cur.fetchall()
        partner_names = [row[0] for row in partners]

        partner = 'OZON'
        type_proj = 'Активный'

        cur.execute("""
            SELECT mocp.title
            FROM mv_outcoming_call_project mocp
            WHERE mocp.partnername = %s
            and mocp.state = %s
            and mocp.removed = false""", (partner, type_proj,))

        proj_names = cur.fetchall()
        df_projects = pd.DataFrame(proj_names, columns=['title'])
        df_projects = df_projects['title'].tolist()

        markup = types.ReplyKeyboardMarkup(row_width=1)
        for project in df_projects[:110]:
            markup.add(types.KeyboardButton(project))
        bot.send_message(message.chat.id, f'Привет {message.from_user.full_name}', reply_markup=markup)

    @bot.message_handler(func=lambda message: True)
    def menu(message):
        selected_project = message.text     #Название проекта

        cur.execute("""
            SELECT mocp.uuid, mocp.formtemplate
            FROM mv_outcoming_call_project mocp
            WHERE mocp.title = %s
            and mocp.removed = false
            """, (selected_project,))

        selected_data = cur.fetchall()

        df_selected = pd.DataFrame(selected_data, columns=['uuid', 'formtemplate'])
        entry_project = df_selected['uuid'].values[0]
        entry_anket = df_selected['formtemplate'].values[0]

        # Ключи для анкеты
        cur.execute("""
        select mcft.identifier_, mcft.groupid, mcft.title
        from mv_custom_form_template mcft 
        where formtemplate = %s
        """, (entry_anket,))
        anket_keys = cur.fetchall()
        df_keys = pd.DataFrame(anket_keys, columns=['identifier_', 'groupid', 'title'])
        # print(df_keys)
        cur.execute("""
        select ci.case_uuid, ci.phonenumbers, ci.stringvalue2, ci.statetitle, ci.cont, 
        ci.speak, ci.wrap, ci.hold_, ci.operatortitle,
        ci.casecomment, ci.results, mcf.jsondata 
        from 
        (
        select case_uuid, casecomment, statetitle, phonenumbers, stringvalue2,
        operatortitle, count(*) as cont,
        string_agg(concat(result, '\n', attempt_start),';\n' order by attempt_start) as results,
        sum(speakingtime) as speak, sum(wrapup_time) as wrap, sum(hold_) as hold_
        from
        (select
          case_uuid, 
          case_.casecomment,
          case_.statetitle,
          case_.phonenumbers,
          case_.stringvalue2,
          dos.session_id as session_id,
          attempt_start as attempt_start,
          client_number as abonent,
          coalesce(speaking_time,0) as speakingtime,
          dos.wrapup_time,
          intervaltosec(cs.ended - cs.entered) as hold_,
          case_.operatortitle,
            (case when ph.callDispositionTitle is not null then ph.callDispositionTitle 
          else ':adhoc-report.attemp_result.'||attempt_result||':' end) as result
        from detail_outbound_sessions dos
          join mv_call_case case_ on (case_.uuid = case_uuid)
          left join mv_custom_form form_ on (form_.owneruuid = case_uuid and form_.removed = false)
          left join mv_catalog_item item on lower(number_type)=lower(item.code) and item.catalogowneruuid is null and item.catalogcode = 'PhoneTypes'
          left join mv_phone_call ph on (ph.callcaseuuid = case_.uuid and ph.sessionid = session_id)
          left join call_status cs on dos.session_id = cs.session_id and cs.initiator_id = dos.login 
          left join mv_employee me on dos.login = me.login 
        where
          dos.project_id = %s
          limit 500)sub
        group by 1,2,3,4,5,6) ci
        left join mv_custom_form mcf 
        on mcf.owneruuid = ci.case_uuid
        and mcf.removed = false
        """, (entry_project,))
        main_query = cur.fetchall()

        # Данные по звонкам проекта
        df_calls = pd.DataFrame(main_query,
                                columns=['case_uuid', 'phonenumbers', 'stringvalue2', 'statetitle', 'cont', 'speak',
                                         'wrap', 'hold_', 'operatortitle', 'casecomment', 'results', 'jsondata'])

        # Закрытие соединения
        # cur.close()
        # conn.close()
        # df_json = df_calls['jsondata']
        #
        json_keys = df_keys['identifier_'].tolist()
        anket_name = df_keys['groupid'].drop_duplicates().item()
        column_names = df_keys['title'].tolist()

        data = []

        for json_data in df_calls['jsondata']:
            if isinstance(json_data, dict):
                if anket_name in json_data:
                    sub_data = json_data[anket_name]

                    row_data = {}

                    for key in json_keys:
                        if key in sub_data:
                            row_data[key] = sub_data[key]
                        else:
                            row_data[key] = None

                    data.append(row_data)
                else:
                    row_data = {key: None for key in json_keys}
                    data.append(row_data)
            else:
                row_data = {key: None for key in json_keys}
                data.append(row_data)

        df_result = pd.DataFrame(data)
        df_result = df_result.astype(str).applymap(lambda x: x.replace("['", '').replace("']", ''))

        mapping = dict(zip(json_keys, column_names))
        df_result = df_result.rename(columns=mapping)

        df_combined = pd.concat([df_calls, df_result], axis=1)
        df_combined = df_combined.rename(columns={'case_uuid': 'Кейс', 'phonenumbers': 'Номер телефона',
                                                  'stringvalue2': 'Внешний идентификатор',
                                                  'statetitle': 'Состояние кейса',
                                                  'cont': 'Количество звонков', 'speak': 'Общее время разговора',
                                                  'wrap': 'Общая постобработка',
                                                  'hold_': 'Общее ожидание', 'operatortitle': 'Оператор',
                                                  'casecomment': 'Комментарий к кейсу',
                                                  'results': 'Результат/Время вызова'})
        df_combined.drop('jsondata', axis=1, inplace=True)
        df_combined['Последний звонок'] = df_combined['Результат/Время вызова'].str.rsplit(';\n', n=1).str[-1]
        columns = df_combined.columns.tolist()

        column_to_move = 'Последний звонок'
        after_column = 'Оператор'

        if column_to_move in columns and after_column in columns:
            columns.remove(column_to_move)
            insert_index = columns.index(after_column) + 1
            columns.insert(insert_index, column_to_move)
            df_combined = df_combined[columns]
        else:
            print("Один из указанных столбцов не найден в DataFrame.")
        # Разделение столбца "Результат/Время вызова" по разделителю ";\n" и создание новых столбцов c префиксом, индексируя от 1
        all_results = df_combined['Результат/Время вызова'].str.split(';\n', expand=True)

        all_results = all_results.rename(columns=lambda x: f"Результат/Время вызова {int(x) + 1}")

        df_combined = pd.concat([df_combined, all_results], axis=1)

        df_combined.drop('Результат/Время вызова', axis=1, inplace=True)
        pd.ExcelWriter(fr'{selected_project}.xlsx')

        df_combined.to_excel(fr'{selected_project}.xlsx')
        with open(os.path.abspath(fr"{selected_project}.xlsx"), 'rb') as f:
            bot.send_document(message.chat.id, f)
        os.remove(fr'{selected_project}.xlsx')

    bot.infinity_polling()



    # @bot.message_handler(func=lambda message: message.chat.id not in users)
    # def some(message):
    #     bot.send_message(message.chat.id, "У вас нет прав на данную операцию, пришлите Косте свой telegramID 😎 "
    #                                       "Узнать свой ID можно у этого бота https://web-telegram.ru/#@my_id_bot")
    #
    #


if __name__ == '__main__':
    bot = telebot.TeleBot(TOKEN)
    users = [699765581, 330823635, 465164262, 545198787, 433582205,
             270077112, 1667349733, 180292423, 787467162, 678277088,
             784709252, 428527360, 2145402995, 1333892894]

    conn = psycopg2.connect(database="nccrep", user="readonly", password="xpr5z5oUoTTkvYuK0IP2",
                            host="telesales-service.nau.team", port="5432")
    cur = conn.cursor()

    main(bot, cur)
