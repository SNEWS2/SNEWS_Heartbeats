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
def run_heartbeat():
    """ Initiate Coincidence Decider
    """
    hb = HeartBeat()
    try:
        hb.subscribe()
    except KeyboardInterrupt:
        pass
    finally:
        click.secho(f'\n{"="*30}DONE{"="*30}', fg='white', bg='green')