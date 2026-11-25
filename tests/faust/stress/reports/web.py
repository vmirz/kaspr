from jinja2 import Environment, PackageLoader, select_autoescape

env = Environment(
    loader=PackageLoader('tests.faust.stress.reports', 'templates'),
    autoescape=select_autoescape(['html', 'xml']),
)
