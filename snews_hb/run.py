
from snews_hb.heartbeat import HeartBeat
import click

def run_heartbeat_tracker():
    hb = HeartBeat()
    try:
        hb.subscribe()
    except KeyboardInterrupt:
        pass
    finally:
        click.secho(f'\n{"= " *30}DONE{"= " *30}', fg='white', bg='green')

if __name__ == "__main__":
    run_heartbeat_tracker()
