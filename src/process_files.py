try:
    import unzip_requirements
except ImportError:
    pass

import json
import os
from itertools import islice

import boto3
from smart_open import smart_open

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

    def __process_lines(self, lines):
        self.__classify_lines(lines=lines)
        self.__handle_queries()

    def __classify_lines(self, lines):
        for line in lines:
            line_words = line.split(";")

            if not line_words[8]:
                tag = "NULL"
            else:
                tag = line_words[8]

            line_data = self.__get_line_data(line_words=line_words, tag=tag)

            if line_words[6] == "Enviado":
                self.sent_values_list.append(line_data)

            if line_words[6] == "Click":
                self.click_values_list.append(line_data)

            if line_words[6] == "Abierto":
                self.open_values_list.append(line_data)

            if line_words[6] == "Desuscripto":
                self.unsubscribe_values_list.append(line_data)

    @staticmethod
    def __get_line_data(line_words, tag):
        return (
            line_words[0],
            line_words[1],
            line_words[2],
            line_words[3].replace("'", " "),
            line_words[4].replace("'", " "),
            line_words[7].replace("'", " "),
            tag
        )

    def __handle_queries(self):
        if self.unsubscribe_values_list:
            self.db_instance.handler(
                query=self.__get_unsubscribe_query(values=self.unsubscribe_values_list)
            )
            self.unsubscribe_values_list.clear()

        if self.click_values_list:
            self.db_instance.handler(query=self.__get_click_query(values=self.click_values_list))
            self.click_values_list.clear()

        if self.open_values_list:
            self.db_instance.handler(query=self.__get_open_query(values=self.open_values_list))
            self.open_values_list.clear()

        if self.sent_values_list:
            self.db_instance.handler(query=self.__get_sent_query(values=self.sent_values_list))
            self.sent_values_list.clear()

    def __get_unsubscribe_query(self, values):
        return self.build_insert_query(
            table="em_blue_unsubscribe_event",
            columns=self.__get_columns(),
            values=values
        )

    def __get_click_query(self, values):
        return self.build_insert_query(
            table="em_blue_link_click_event",
            columns=self.__get_columns(url=1),
            values=values
        )

    def __get_open_query(self, values):
        return self.build_insert_query(
            table="em_blue_open_email_event",
            columns=self.__get_columns(),
            values=values
        )

    def __get_sent_query(self, values):
        return self.build_insert_query(
            table="em_blue_sent_email_event",
            columns=self.__get_columns(),
            values=values
        )

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
    def build_insert_query(table: str, columns, values) -> str:
        return f"""
            INSERT INTO {table}({", ".join([str(c) for c in columns])})
            VALUES {values};
        """.replace(
            "[", ""
        ).replace(
            "]", ""
        )


def handler(event, context):
    process_file = ProcessFile(file_name=event["Records"][0]["s3"]["object"]["key"])
    process_file.executor()
    return {"statusCode": 200}
