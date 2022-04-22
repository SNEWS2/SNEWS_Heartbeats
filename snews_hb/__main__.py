import click, os
from . import __version__
from .heartbeat import HeartBeat

@click.group(invoke_without_command=True)
@click.version_option(__version__)
@click.pass_context
def main(ctx):
    """ User interface for snews_pt tools
    """
    base = os.path.dirname(os.path.realpath(__file__))
    ctx.ensure_object(dict)
    ctx.obj['env'] = "env"

@main.command()
@click.option('--output_folder', '-o', default='cwd', show_default='cwd', help='Output folder')
@click.option('--store', '-s', default='both', show_default='both', help='either ["both", "csv", "json"]')
def run_heartbeat(output_folder, store):
    """ Initiate Coincidence Decider
    """
    if output_folder == 'cwd':
        output_folder = None

    hb = HeartBeat(logs_folder=output_folder, store=store)
    try:
        hb.subscribe()
    except KeyboardInterrupt:
        pass
    finally:
        click.secho(f'\n{"="*30}DONE{"="*30}', fg='white', bg='green')