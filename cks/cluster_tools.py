#from IPython import parallel
import ipyparallel as parallel

try:
    clients = parallel.Client(profile="para")
except IOError, e:
    raise Exception('Please initialize cluster cores on the "para" profile.')
clients.block = False  # synchronous computation
print clients.ids
view = clients.load_balanced_view()
clients.purge_everything()

def get_map_runstats(res, mp):
    """returns (html, runstats dict)"""
    runstats = {'run_progress': '%d / %d' % (res.progress, len(mp)),
            'run_time': res.elapsed,
            'run_ncores': len(clients)}
    html = """<table class="u-pull-left sk">
    <thead><tr><th>#cores</th><th>progress</th><th>runtime</th></tr></thead>
    <tbody>
    <tr><td>{run_ncores}</td><td>{run_progress}</td><td>{run_time:.2f} s</td></tr>
    </tbody>
    </table>
    """.format(**runstats)
    if res.ready():
        if res.successful():
            html += '<span class="conclusion-p u-pull-right">FINISHED</span>'
        else:
            html += '<span class="conclusion-n u-pull-right">Finished with errors</span>'
    else:
        html += '<span class="conclusion u-pull-right">running...</span>'
    return (html, runstats)
