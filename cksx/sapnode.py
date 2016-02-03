#!/usr/bin/env python3

from flask import stream_with_context, request, Response
from flask import Flask
import json
from time import sleep
import sys
import sapserver

app = Flask(__name__)


@app.route('/')
def hello_world():
    sleep(0.5)
    return 'UP'


@app.route('/test')
def test():
    sleep(1)
    return


@app.route('/meta', methods=['POST'])
def meta():
    return Response(sapserver.get_meta(**json.loads(request.data)))


@app.route('/read', methods=['POST'])
def read():
    return Response(sapserver.read(**json.loads(request.data)))


@app.route('/info', methods=['POST', 'GET'])
def info():
    return Response(sapserver.info(**json.loads(request.data)))

if __name__ == '__main__':
    sys.argv[0]
    app.debug = True
    try:
        port = int(sys.argv[1])
    except IndexError:
        port = 5000
    app.run(host='0.0.0.0', port=port)
