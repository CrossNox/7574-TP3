# Instalación del entorno de desarrollo
## Pre requisitos
El proyecto fue empaquetado con [poetry](https://python-poetry.org/) para manejar dependencias cómodamente. Puede seguir la [guía de instalación](https://python-poetry.org/docs/#installation) para instalar la herramienta.

## Instalación
Para instalar el paquete, sus dependencias, y las dependencias de desarrollo, debe ejecutar el siguiente comando:

```bash
poetry install
```

### Instalar hooks de pre-commit
El proyecto usa [pre-commit](https://pre-commit.com) para correr diferentes linters, formateadores, la suite de tests, y otros hook. Para instalar los hooks, se deben ejecutar los siguientes comandos:

```bash
poetry run pre-commit install
poetry run pre-commit install -t pre-push
```

# Guia de estilo
El proyecto sigue [PEP8](https://www.python.org/dev/peps/pep-0008/).

Si se instalaron los [hooks de pre-commit](#instalar-hooks-de-pre-commit), no debiera ser una preocupación esta sección. Varios de los hooks se encargan de arreglar el código o advertir sobre errores de estilo. Se usan los siguientes hooks:

- [black](https://github.com/psf/black): `An opinionated code formatting tool that ensures consistency across all projects using it`
- [mypy](https://github.com/python/mypy): `A static type checker for Python`
- [pylint](https://github.com/PyCQA/pylint): `A source code, bug and quality checker`

# Docstrings
El proyecto utiliza la [guía de estilos de google](https://github.com/google/styleguide/blob/gh-pages/pyguide.md#38-comments-and-docstrings) para docstrings.

# Versionado
El proyecto utiliza [SemVer](https://semver.org). El manejo de versiones se hace a través de `poetry`. Para subir la versión del paquete el comando es `poetry version <patch|minor|major>`.

## Changelog
El archivo `CHANGELOG.md` mantiene un log de los cambios relevantes del proyecto, siguiendo la guía de [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

# Diagramas de clase y de paquete
Para generar los diagramas a través de `pyreverse` y guardarlos en el lugar correspondiente:

```bash
poetry run pyreverse lazarus -o png
mv classes.png informe/images/classes.png
mv packages.png informe/images/packages.png
```
