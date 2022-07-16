import os
folders = {
    "component" : ["data_ingestion.py","data_transformation.py","data_validation.py","model_trainer.py","model_evaluation.py","model_pusher.py","__init__.py"],
    "config" : ["configuration.py","__init__.py"],
    "constant" : ["__init__.py"],
    "entity" : ["config_entity.py","artifact.py","model_factory.py","__init__.py"],
    "exception" : ["__init__.py"],
    "logger" : ["__init__.py"],
    "pipeline" : ["pipeline.py","__init__.py"],
    "util" : ["util.py","__init.py"]
}

print(os.getcwd())

root = os.getcwd()
folds = os.path.join(root,'shipment')
print(folds)

for k,v in folders.items():
    for i in v:
        os.makedirs(os.path.join(folds,k),exist_ok=True)
        file = os.path.join(folds,k,i)
        with open(file,"w") as f:
            pass
        f.close()

print('done')