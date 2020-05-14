"""Launch excel macros"""

import logging
import win32com.client

from os import path


def report() -> None:
    """Формирование еженедельных отчетов в эксель файлах"""
    report_path = r".\support_data\reports\Анализ_конструкторов.xlsm"
    macro(report_path)


def macro(path_: str) -> None:
    """Запуск макроса в excel файле.
    !!!!!!!Макрос всегда в модуле 1 и называется load_data!!!!!!!!

    :param path_: относительнвый путь к excel файлу с макросом
    """
    logging.info("Launch excel macros")

    abs_path = path.abspath(path_)
    name_file = path.basename(abs_path)
    if path.exists(abs_path):
        excel_macro = win32com.client.DispatchEx("Excel.Application")  # DispatchEx is required in the newest versions of Python.
        excel_path = path.expanduser(abs_path)
        workbook = excel_macro.Workbooks.Open(Filename=excel_path, ReadOnly=1)
        excel_macro.Application.Run(f"{name_file}!Module1.load_data")
        workbook.Save()
        excel_macro.Application.Quit()
        del excel_macro
