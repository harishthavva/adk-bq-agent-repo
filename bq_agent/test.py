from google.adk.apps.app import App

adk_app = App(name="bq_app", root_agent="agent")

print([m for m in dir(adk_app) if not m.startswith("_")])
