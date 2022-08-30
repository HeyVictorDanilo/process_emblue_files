import pytest

from src.process_files import ProcessFile


@pytest.fixture()
def get_correct_file_name():
    return ""


@pytest.fixture()
def get_process_file_class(get_correct_file_name):
    return ProcessFile(file_name=get_correct_file_name)


@pytest.fixture()
def build_process_file_object(get_correct_file_name):
    process_file = ProcessFile(file_name=get_correct_file_name)
    return process_file


@pytest.fixture()
def build_empty_process_file_object():
    process_file = ProcessFile(file_name="")
    return process_file


@pytest.fixture()
def get_lines_process():
    return [
        "EMAIL;FECHA_ENVIO;FECHA_ACTIVIDAD;CAMPANIA;ACCION;TIPO_ACCION;ACTIVIDAD;DESCRIPCION;TAG",
        "adjorozco@gmail.com;2022-08-16 08:59:04.000;2022-08-16 08:59:09.000;Aquí te cuento;20220816 - Aquí te cuento ;Envio simple;Enviado;null, null",
        "adjorozco@gmail.com;2022-08-16 08:59:04.000;2022-08-16 08:59:09.000;Aquí te cuento;20220816 - Aquí te cuento ;Envio simple;Enviado;null, null",
        "adjorozco@gmail.com;2022-08-16 08:59:04.000;2022-08-16 08:59:09.000;Aquí te cuento;20220816 - Aquí te cuento ;Envio simple;Open;null, null",
        "adjorozco@gmail.com;2022-08-16 08:59:04.000;2022-08-16 08:59:09.000;Aquí te cuento;20220816 - Aquí te cuento ;Envio simple;Unsubscribe;null, null",
        "adjorozco@gmail.com;2022-08-16 08:59:04.000;2022-08-16 08:59:09.000;Aquí te cuento;20220816 - Aquí te cuento ;Envio simple;Enviado;null, null",
        "adjorozco@gmail.com;2022-08-16 08:59:04.000;2022-08-16 08:59:09.000;Aquí te cuento;20220816 - Aquí te cuento ;Envio simple;Click;null, null",
    ]


def test_initial_arrays(build_process_file_object):
    assert build_process_file_object.sent_values_list == []
    assert build_process_file_object.unsubscribe_values_list == []
    assert build_process_file_object.open_values_list == []
    assert build_process_file_object.click_values_list == []


def test_initial_counters(build_process_file_object):
    assert build_process_file_object.click_counter == 0
    assert build_process_file_object.sent_counter == 0
    assert build_process_file_object.open_counter == 0
    assert build_process_file_object.unsubscribe_counter == 0


def test_db_instance(build_process_file_object):
    assert build_process_file_object.db_instance


@pytest.mark.xfail
def test_handler_fail(build_empty_process_file_object):
    assert build_empty_process_file_object.executor()


def test_handler(build_process_file_object):
    assert build_process_file_object.executor()


def test_classify_lines(get_process_file_class, get_lines_process):
    assert get_process_file_class._ProcessFile__classify_lines(lines=get_lines_process)

