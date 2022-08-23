try:
    import unzip_requirements
except ImportError:
    pass

import os
from itertools import islice

import boto3
from botocore.exceptions import ClientError
import logging
from smart_open import smart_open
from psycopg2 import sql
from src.main_db import DBInstance


class ProcessFile:
    def __init__(self, file_name: str):
        self.client = boto3.client(
            service_name="s3",
            region_name=os.getenv("REGION"),
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
        )
        self.file_name = file_name
        self.db_instance = DBInstance(public_key=os.getenv("CLIENT_KEY"))
        self.unsubscribe_values_list = []
        self.open_values_list = []
        self.sent_values_list = []
        self.click_values_list = []
        self.open_counter = 0
        self.sent_counter = 0
        self.click_counter = 0
        self.unsubscribe_counter = 0

    def executor(self):
        with smart_open(
            f's3://{os.getenv("BUCKET_CSV_FILES")}/{self.file_name}',
            "rb",
            encoding="utf-16",
        ) as file:
            while True:
                lines = list(islice(file, 1000))
                self.__process_lines(lines=lines)
                if not lines:
                    break
        self.__delete_csv_file()

    def __delete_csv_file(self):
        try:
            self.client.delete_object(
                Bucket=os.getenv("BUCKET_CSV_FILES"),
                Key=self.file_name,
            )
        except ClientError as e:
            logging.error(e)
        else:
            logging.info("Deleted csv file")

    def __process_lines(self, lines):
        self.__classify_lines(lines=lines)
        self.__handle_queries()

    def __classify_lines(self, lines):
        for line in lines:
            line_words = line.split(";")

            line_data = self.__get_line_data(line_words=line_words)

            if line_words[6] == "Enviado":
                self.sent_values_list.extend(line_data)
                self.sent_counter += 1
            elif line_words[6] == "Click":
                self.click_values_list.extend(line_data)
                self.click_counter += 1
            elif line_words[6] == "Abierto":
                self.open_values_list.extend(line_data)
                self.open_counter += 1
            elif line_words[6] == "Desuscripto":
                self.unsubscribe_values_list.extend(line_data)
                self.unsubscribe_counter += 1
            else:
                logging.info(line_words[6])

    @staticmethod
    def __get_line_data(line_words):
        if line_words[8] == '\x00\n' or line_words[8] == '\x00':
            tag = "NULL"
        else:
            tag = line_words[8]

        if line_words[7] == '\x00\n' or line_words[7] == '\x00':
            description = "NULL"
        else:
            description = line_words[7]

        return [
            line_words[0],
            line_words[1],
            line_words[2],
            line_words[3],
            line_words[4],
            description,
            tag
        ]

    def __handle_queries(self):
        try:
            if self.unsubscribe_values_list:
                self.db_instance.handler(
                    query=self.get_query(
                        table="em_blue_unsubscribe_event",
                        columns=self.__get_columns(),
                        values=self.unsubscribe_counter
                    ),
                    params=self.unsubscribe_values_list
                )
                self.unsubscribe_values_list.clear()
                self.unsubscribe_counter = 0

            if self.click_values_list:
                self.db_instance.handler(
                    query=self.get_query(
                        table="em_blue_link_click_event",
                        columns=self.__get_columns(url=1),
                        values=self.click_counter
                    ),
                    params=self.click_values_list
                )
                self.click_values_list.clear()
                self.click_counter = 0

            if self.open_values_list:
                self.db_instance.handler(
                    query=self.get_query(
                        table="em_blue_open_email_event",
                        columns=self.__get_columns(),
                        values=self.open_counter
                    ),
                    params=self.open_values_list
                )
                self.open_values_list.clear()
                self.open_counter = 0

            if self.sent_values_list:
                self.db_instance.handler(
                    query=self.get_query(
                        table="em_blue_sent_email_event",
                        columns=self.__get_columns(),
                        values=self.sent_counter
                    ),
                    params=self.sent_values_list
                )
                self.sent_values_list.clear()
                self.sent_counter = 0

        except Exception as e:
            raise e
        else:
            logging.info("Sent queries")

    @staticmethod
    def __get_columns(url=0):
        if url == 1:
            return [
                "email",
                "sent_date",
                "activity_date",
                "campaign",
                "action",
                "url",
                "tag",
            ]
        else:
            return [
                "email",
                "sent_date",
                "activity_date",
                "campaign",
                "action",
                "description",
                "tag",
            ]

    @staticmethod
    def get_query(table: str, columns, values: int):
        columns = [sql.Identifier(c) for c in columns]
        values = [sql.SQL(', ').join(sql.Placeholder() * len(columns)) for v in range(0, values)]

        return sql.SQL("INSERT INTO {table}({columns}) VALUES ({values});").format(
            table=sql.Identifier(table),
            columns=sql.SQL(', ').join(columns),
            values=sql.SQL('), (').join(values)
        )


def handler(event, context):
    try:
        process_file = ProcessFile(file_name=event["Records"][0]["s3"]["object"]["key"])
        process_file.executor()
    except Exception as e:
        logging.error(str(e))
        return {"statusCode": 400}
    else:
        return {"statusCode": 200}