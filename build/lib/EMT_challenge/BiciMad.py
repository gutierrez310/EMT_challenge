import csv
import pandas as pd
import numpy as np
from datetime import datetime
from ast import literal_eval
from .UrlEMT import UrlEMT

class BiciMad():
    def __init__(self, month, year):
        # * `__init__`: método constructor. Acepta los argumentos de tipo entero `month` y `year`.
        # El atributo que representa los datos, se actualiza al construir el objeto mediante el método estático `get_data`.
        self._my_UrlEMT = UrlEMT()
        self.interesting_csv = []
        self.month = month
        self.year = year
        self._data = []
        self.res = []

    def get_data(self, month, year, delimiter=";"):
        # * `get_data`: método estático que acepta los argumentos de tipo entero `month` y `year`
        # y devuelve un objeto de tipo DataFrame con los datos de uso correspondientes al mes
        # `month` y año  `year`. El índice del dataframe debe ser la fecha en la que tuvo lugar
        # el viaje. Todas las fechas que aparecen en los datos ha de tener tipo `datetime`.
        # Las únicas columnas que tiene que tener el DataFrames son:
        # ```
        # [ 'idBike', 'fleet', 'trip_minutes', 'geolocation_unlock', 'address_unlock',
        #   'unlock_date', 'locktype', 'unlocktype', 'geolocation_lock', 'address_lock', 'lock_date', 'station_unlock',
        #   'unlock_station_name', 'station_lock', 'lock_station_name']
        # ```
        self.month = month
        self.year = year
        self.interesting_csv = self._my_UrlEMT.get_csv(month, year)
        self._data = []
        for csvfile in self.interesting_csv:
            if 'csv' in str(csvfile):
                interesting = ['idBike', 'fleet', 'trip_minutes', 'geolocation_unlock', 'address_unlock', 'unlock_date', 'locktype',
                              'unlocktype', 'geolocation_lock', 'address_lock', 'lock_date', 'station_unlock', 'unlock_station_name',
                              'station_lock', 'lock_station_name']
                # my_dtypes=[np.float64, np.float64, np.float64, literal_eval, str, datetime.fromisoformat,
                #           str, str, literal_eval, str, datetime.fromisoformat, np.float64, np.float64,
                #           str, np.float64, np.float64, str]
                
                read = csv.reader(csvfile, delimiter=delimiter)
                header = read.__next__()[1:]
                rows = []
                for line in read:
                    rows.append(line)
                rows = np.array(rows).T.tolist()  # garbage collectors job
                fecha_index = rows.pop(0)

                # for idx in range(len(rows)):
                #   rows[idx] = list(map(lambda val: self._my_generic_parser(val, my_dtypes[idx]), rows[idx]))

                res = pd.DataFrame(np.array(rows).T, columns=header, index=pd.to_datetime(fecha_index, format="%Y-%m-%d"))
                self.res.append(res)
                try:
                    self._data.append(res[interesting])
                except NameError:
                    raise NameError("Could not find expected columns in the data. Saved loaded data in self.res.")
                continue
            if 'json' in str(csvfile):
                res = pd.read_json(csvfile, lines=True)
                self._data.append(res)
                continue
            print("Got something an invalid file type")
        return None

    @property
    def data(self):
        # * `data`: método decorado con el decorador `@property` para acceder al atributo que representa los datos de uso. El atributo ha de llamarse igual.
        return self._data.copy()

    def __str__(self):
        # * `__str__`: método especial que permite la representación informal del objeto. Su comportamiento es idéntico al método `__str__` de los objetos de la clase DataFrame.
        if not self._data:
            return None
        return self._data[0].__str__()

    def clean(self):
        # * `clean`: método de instancia que se encarga de realizar la limpieza  y transformación del dataframe que representa los datos. Modifica el dataframe y no devuelve nada. Realiza las siguientes tareas:
        #     * Borrado de valores NaN. Borrar las filas con todos sus valores NaN.
        #     * Cambiar el tipo de datos de las siguientes columnas del dataframe: `fleet`, `idBike`, `station_lock`, `station_unlock`. Dichas columnas han de ser de tipo `str`.
        # En este caso, podrás aprovechar el código ya implementado en la sección anterior.

        print("Cleaning data.")
        for idx in range(len(self._data)):
            if not isinstance(self._data[idx], pd.DataFrame): # will not clean data if its not csv (not the same columns etc)
                continue
            self._delete_nan_rows(idx, self._data)
            for col_name in ['fleet', 'idBike', 'station_lock', 'station_unlock']:
                self._float_to_str(idx, self._data, col_name)
        return None

    def resume(self, **kwargs):
        # * `resume`: método de instancia que devuelve un objeto de tipo Series con las siguientes restricciones:
        #     * el índice está formado con las etiquetas:
        #         'year', 'month', 'total_uses', 'total_time', 'most_popular_station', 'uses_from_most_popular'
        #     * los valores son: el año, el mes, el total de usos en dicho mes, el total de horas en dicho mes,
        #       el conjunto de estaciones de bloqueo con mayor número de usos y el número de usos de dichas estaciones.

        # En este caso podrás aprovechar el código de las funciones implementadas en la sección de consultas.
        self._data= []
        if kwargs.get('month') and kwargs.get('year'):
            self.month = kwargs.get('month')
            self.year = kwargs.get('year')

        self.get_data(self.month, self.year)
        self.clean()
        for idx in range(len(self._data)):
            if '.json' in str(self.interesting_csv[idx]):
                print(f"We found a json and saved it in self.data[{idx}].")
                continue
            for key, value in kwargs.items():
                match(key):
                    case 'year':
                        print(f"year: {self.year}")
                    case 'month':
                        print(f"month: {self.month}")
                    case 'total_uses':
                        print("total_uses: ")
                        print(self._count_usage(idx, self._data))
                    case 'total_time':
                        print(f"total_time: {self._time_of_usage(idx, self._data)} hours")
                    case 'most_popular_station':
                        print(f"most_popular_station: ")
                        print(self._most_popular_stations(self._data[idx]))
                    case 'uses_from_most_popular':
                        print('uses_from_most_popular: ')
                        print(self._usage_from_most_popular_station(self._data[idx]))
        return None

    def _my_generic_parser(self, val, a_dtype):
        if val == "":
            return np.nan
        else:
            return a_dtype(val)

    def _delete_nan_rows(self, idx, l_df, how='all'):
        l_df[idx] = l_df[idx].dropna(how=how)
        return None

    def _float_to_str(self,  idx, l_df, col_name):
        if col_name not in l_df[idx].columns:
          return None
        df = l_df[idx].copy()
        df[col_name] = df[col_name].astype({col_name: 'string'})
        l_df[idx] = df
        return None

    def _count_usage(self,  idx, l_df):
        return l_df[idx]['unlocktype'].value_counts().values[0]

    def _time_of_usage(self, idx, l_df):
        return sum(l_df[idx]['trip_minutes'].values)/60

    def _most_popular_stations(self, df):
        df = df.reset_index()
        df1 = df.groupby('unlock_station_name').count().sort_values(by='idBike', ascending=False)['idBike'].copy()
        popular_unlock_st = df1.reset_index()['unlock_station_name'].values
        address = []
        for pus in popular_unlock_st:
            address.append(df[df['unlock_station_name'] == pus]["address_unlock"].values[0])
        return pd.Series(data=address, index=popular_unlock_st, name='most_popular_stations')

    def _usage_from_most_popular_station(self, df):
        df = df.reset_index()
        return df.groupby('unlock_station_name').count().sort_values(by='idBike', ascending=False)['idBike'].copy()
