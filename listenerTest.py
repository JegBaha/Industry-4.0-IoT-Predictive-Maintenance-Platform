import paho.mqtt.client as mqtt
def on_message(c,u,m): print(m.topic, m.payload.decode())
c=mqtt.Client(); c.connect("localhost",1883); c.subscribe("factory/#"); c.on_message=on_message; c.loop_forever()
