# Lazarus
## High Availability Reddit Memes Analyzer
TP3 para 7574 - Distribuidos I.

# Arquitectura
Referirse a `informe/informe.pdf`.

# Instalación
El proyecto fue empaquetado con [poetry](https://python-poetry.org/) para manejar dependencias cómodamente. Puede seguir la [guía de instalación](https://python-poetry.org/docs/#installation) para instalar la herramienta.

Teniendo `poetry` instalado, el siguiente comando creará un nuevo entorno para poder ejecutar el proyecto:

```bash
poetry install --no-dev
```

# Descarga del set de datos
El subcomando `lazarus dataset` sirve para descargar la data. Se requiere tener un `json` de autenticación de la API de Kaggle. Para descargarlo, se deben seguir los pasos bajo la sección `Authentication` de [esta guía](https://www.kaggle.com/docs/api). También genera los datasets reducidos.

Su ejecución se puede simplificar con `make download-dataset`. Se espera tener el json de autenticación en la ubicación estándar (`~/.kaggle`). Se puede cambiar el tamaño del dataset reducido con `make download-dataset SAMPLE_SIZE=<f>`, reemplazando `<f>` por un valor en el rango `(0, 1)`.

# Configuración
En el root del proyecto se provee un archivo `sample_settings.ini` con los posibles valores de configuración. Sin embargo, el archivo esperado se llama `settings.ini`. Por motivos obvios de seguridad, este archivo es ignorado en el sistema de versionado con `.gitignore`.

Puede copiar el archivo de prueba provisto, renombrarlo y modificar los valores según necesidad.

Cada posible configuración se puede sobreescribir con variables de entorno con la nomenclatura`<Seccion>_<Clave>`. Por ejemplo `SERVER_HOST`.

# Ejecución
Revisar los subcomandos disponibles y sus usos con:

```bash
poetry run lazarus --help
```
