from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'josko',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'WidgetPipelineGraph', default_args=default_args, schedule_interval=timedelta(minutes=10))



# So, what I want to do is just list relations. I don't care otherwise. This thing should figure it out from there.
# Just throw them in here
relations = []
relations.append({"EDW.first_thing": "presentation.second_thing"})
relations.append({"importers.third_thing": "presentation.second_thing"})
relations.append({"EDW.first_thing": "presentation.fourth_thing"})
relations.append({"importers.third_thing": "presentation.fourth_thing"})

custom_colors = {
    "kubernetes": {
        "background": "#0000ff",
        "font": "#ffffff"
    }
}



unique_values = []
for relation in relations:
    for key, value in relation.items():
        unique_values.append(key)
        unique_values.append(value)

unique_values = list(set(unique_values))


entities = {}

for entity_name in unique_values:
    print(f"Creating entity {entity_name}.")
    entities[entity_name] = DummyOperator(task_id=entity_name, dag=dag)

    dataset = entity_name.split(".")[0]

    if dataset in custom_colors:
        custom_color = custom_colors.get(dataset)

        entities[entity_name].ui_color = custom_color['background']
        entities[entity_name].ui_fgcolor = custom_color['font']
    else:
        entities[entity_name].ui_color = "#afd34d"
        entities[entity_name].ui_fgcolor = '#000000'

for relation in relations:
    for entity_name, downstream_entity in relation.items():

        print(f"Relating {entity_name} >> {downstream_entity}.")

        entities[entity_name].set_downstream(entities[downstream_entity])


