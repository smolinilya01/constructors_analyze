"""ETL data for constructors analyze"""


from pandas import (read_excel, DataFrame, read_csv)
from datetime import datetime, date

DATA_MARKS_PATH = r'.\support_data\support\marks.xlsx'

# Для тестирования
import os
os.chdir(r'W:\Analytics\Илья\!repositories\constructors_analyze')


def all_etl() -> None:
    """Building all data"""
    data = prepare_marks_data()

    amount_positions(data)
    amount_orders(data)
    amount_unique_orders(data)


def prepare_marks_data() -> DataFrame:
    """Prepare data with marks and positions"""
    data = read_excel(
        DATA_MARKS_PATH,
        header=2,
        usecols=[0, 2, 15, 16, 19],
        parse_dates=['Дата создания заказа'],
        dayfirst=True
    )
    data = data.rename(columns={
        'ID заказа': 'id_order',
        'Дата создания заказа': 'date_creating_order',
        'Марка применена': 'mark_implement',
        'Конструктор - создатель марки': 'name_constructor',
        '№ позиции': 'pos_number'
    })
    data = data.query('name_constructor != "Конструктор ПО"')

    return data


def amount_positions(data: DataFrame) -> None:
    """Counting of amount positions per month"""
    data = data.copy()
    data['date_month'] = data['date_creating_order'].map(beginning_month)

    data = data[['date_month', 'name_constructor', 'id_order', 'pos_number']].\
        drop_duplicates().\
        groupby(by=['date_month', 'name_constructor']).\
        count().\
        reset_index()
    del data['id_order']

    data = data.\
        set_index(['date_month', 'name_constructor']).\
        unstack(level=0, fill_value=0)

    data.to_excel(
        r'.\support_data\dumps\amount_positions.xlsx'
    )


def beginning_month(date_var: datetime) -> date:
    """Transforming datetime in format ('2019-06-05 00:00:00' -> '2019-06-01')"""
    year = date_var.year
    month = date_var.month
    return date(year=year, month=month, day=1)


def amount_orders(data: DataFrame) -> None:
    """Counting of amount total orders"""
    data = data.copy()

    data.to_excel(
        r'.\support_data\dumps\amount_positions.xlsx'
    )


def amount_unique_orders(data: DataFrame) -> None:
    """Counting of amount orders without copying"""
    data = data.copy()

    data.to_excel(
        r'.\support_data\dumps\amount_positions.xlsx'
    )
