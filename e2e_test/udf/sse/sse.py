import json

from flask import Flask, Response

app = Flask(__name__)
def format_sse(data: str | None, event=None) -> str:
    if data:
        msg = f'data: {data}\n\n'
    else:
        msg = '\n'
    
    if event is not None:
        msg = f'event: {event}\n{msg}'

    return msg

@app.route('/')
def home():
    return "Hey there!"

@app.route('/graphql/stream',  methods=['POST'])
def stream():
    def eventStream():
        messages = ["Hi", "Bonjour", "Hola", "Ciao", "Zdravo"]
        for msg in messages:
            data = {
                "data": {
                    "greetings": msg
                }
            }
            yield format_sse(json.dumps(data), "next")
        
        yield format_sse(None, "complete")
    return Response(eventStream(), mimetype="text/event-stream")

if __name__ == '__main__':
    app.run(debug=True, host="127.0.0.1", port=4200)