class LoaderError(Exception):
    """
    Базовый класс всех ошибок загрузчиков.
    Нужен для единообразного перехвата в DAG.
    """
    pass


class TemporaryDataUnavailable(LoaderError):
    """
    Данные временно недоступны:
    - цикл GFS ещё не вышел
    - ERA5 ещё не опубликован
    - CDS API временно недоступен
    Airflow должен сделать retry
    """
    pass


class IncompleteDataError(LoaderError):
    """
    Данные получены частично:
    - не все файлы скачались
    - файлы есть, но размер 0
    - временной диапазон неполный
    Нужно cleanup + retry
    """
    pass


class DataValidationError(LoaderError):
    """
    Формально данные есть, но они неконсистентны:
    - нет ожидаемых координат
    - странные размеры
    - несовпадение времён
    Обычно retry, иногда fatal
    """
    pass


class FatalPipelineError(LoaderError):
    """
    Нефиксимая ошибка:
    - баг в коде
    - несовместимые форматы
    - ошибка merge логики
    DAG должен упасть окончательно
    """
    pass
