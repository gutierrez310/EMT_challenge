import requests
import zipfile
import os
from bs4 import BeautifulSoup
import pathlib
import typing

class UrlEMT():
    def __init__(self):
        self.EMT = 'https://opendata.emtmadrid.es'
        self.GENERAL = "/Datos-estaticos/Datos-generales-(1)"
        self.valid_url = []
        self.interesting_urls = []
        self.interesting_csv = []

    def select_valid_urls(self):
        # * `select_valid_urls`: método estático que se encarga de actualizar el atributo
        # de los objetos de la clase. Devuelve un conjunto de enlaces válidos. Si la
        # petición al servidor de la EMT devuelve un código de retorno distinto de
        # 200, la función lanza una excepción de tipo `ConnectionError`.
        response = requests.get(self.EMT+self.GENERAL)
        if not response.status_code == 200:
            raise ConnectionError("Failed to connect to the server of EMT.")
        soup = BeautifulSoup(response.text, 'html.parser')

        self.valid_url = []
        for link in soup.find_all('a'):
            self.valid_url.append(link.get('href'))

        for idx in range(len(self.valid_url)):
            if not self.valid_url[idx]:
                continue
            if self.valid_url[idx][0]=='/':
                self.valid_url[idx] = self.EMT + self.valid_url[idx]

    def get_links(self):
        # * Para extraer los enlaces hay que definir una funcion `get_links` que
        # tome como parámetros un texto HTML y devuelva un conjunto con todos los enlaces.
        # Esta función debe usar expresiones regulares para encontrar los enlaces.
        if not self.valid_url:
            self.select_valid_urls(self)
        return self.valid_url

    def get_url(self, month, year):
        # * `get_url`: método de instancia que acepta los argumentos de tipo entero
        # `month` y `year` y devuelve el string de la URL correspondiente al mes
        # `month` y año `year`.  Si no existe un enlace válido correspondiente al
        # mes `month` y año `year`, se lanzará una excepción de tipo `ValueError`.
        # Se deberá comprobar que el mes y año se corresponden con valores válidos
        # (`month` entre 1 y 12, `year` entre 21 y 23).
        month, year = self._validate_mm_yyyy(month, year)
        interesting_urls = []
        str1 = str(year)[-2:] + "_" + month
        str2 = str(year) + month
        self.select_valid_urls()
        for url in self.valid_url:
            if not url:
                continue
            if str1 in url or str2 in url:  # guardar cual esta y de ahi buscar el csv...
                interesting_urls.append(url)
        if not interesting_urls:
            raise ValueError(" There are no valid url for the provided month and year.")
        self.interesting_urls = interesting_urls
        return interesting_urls

    def get_csv(self, month, year):
        #  `get_csv`: método de instancia que acepta los argumentos de tipo entero
        # `month` y `year` y devuelve  un fichero en formato CSV correspondiente
        # al mes `month` y año `year`.
        # El tipo del objeto devuelto es TextIO. La función lanzará una excepción
        # de tipo `ConnectionError` en caso de que falle la petición al servidor
        # de la EMT. En este caso, se podrá aprovechar el código de la función `csv_from_ZIP`
        # implementada en la sección anterior.

        self.interesting_urls = self.get_url(month, year)
        month, year = self._validate_mm_yyyy(month, year)
        if len(self.interesting_urls) > 1:
            print(f"Found several files for month: {month}. And year: {year}")
        interesting_csv = []
        for url in self.interesting_urls:
            if '.aspx' not in url:
                continue
            interesting_csv.append(self._csv_from_zip(url))
        self.interesting_csv = interesting_csv
        return interesting_csv

    def _validate_mm_yyyy(self, month, year):
        if year < 1900:
            print("Are you sure this is the right year? Maybe switch year and month position")
        if not isinstance(month, int):
            raise ValueError("`month` must be an int")
        if not isinstance(year, int):
            raise ValueError("`year` must be an int")
        m = '0'+str(month) if month < 10 else str(month)
        y = str(year)
        return m, y

    def _csv_from_zip(self, url, where=os.path.abspath('')) -> typing.IO[str]:
        try:
            my_file_name = url.split("/")[-1].split(".")[0]
            where_path = pathlib.Path(where)
            zip_dir = where_path.joinpath(my_file_name + '.zip')
            unzippd = where_path.joinpath(my_file_name)

            # Send a GET request to the URL
            response = requests.get(url, stream=True)

            # Save the content of the response as a file
            with zip_dir.open('wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

            with zipfile.ZipFile(zip_dir, 'r') as zip_ref:
                zip_ref.extractall(where_path.joinpath(my_file_name))

            res_dir = []
            for file in os.listdir(unzippd):
                if '.csv' in file:
                    res_dir = unzippd.joinpath(file)
                    break
                if '.json' in file:
                    res_dir = unzippd.joinpath(file)
                    break
                else:
                    continue
            if not res_dir:
                print("Did not find a valid file overall")

            return open(res_dir, encoding="utf8")

        except requests.exceptions.ConnectionError as e:
            raise ConnectionError("Failed to connect to the server of EMT.")