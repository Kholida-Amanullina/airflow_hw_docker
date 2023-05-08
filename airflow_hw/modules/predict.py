# <YOUR_IMPORTS>
import os
import json
import dill
import logging
import pandas as pd
from os import path
from pydantic import BaseModel


class Form(BaseModel):
    description: str
    fuel: str
    id: int
    image_url: str
    lat: float
    long: float
    manufacturer: str
    model: str
    odometer: int
    posting_date: str
    price: int
    region: str
    region_url: str
    state: str
    title_status: str
    transmission: str
    url: str
    year: int


def load_model(model_file_name: str):
    try:
        model_file_path = path.join(path.dirname(path.dirname(path.abspath(__file__))),
                                    'data', 'models', model_file_name)
        with open(model_file_path, 'rb') as file:
            model = dill.load(file)
        return model
    except Exception as e:
        raise Exception(e)


def predict(model_name: str = None) -> None:
    if not model_name:
        model_name = 'cars_pipe_202305082242.pkl'
    try:
        # Load model from the file with the best model according to pipeline() result
        model = load_model(model_name)

        # Get list of full paths with test data
        forms_dir = path.join(path.dirname(path.dirname(path.abspath(__file__))), 'data', 'test')
        forms = os.listdir(forms_dir)
        forms_path = [path.join(forms_dir, x) for x in forms]

        # Get single predict and union them to one pandas DataFrame
        predicts = pd.DataFrame()
        for form in forms_path:
            with open(form) as f:
                form_json = json.load(f)
            df = pd.DataFrame(form_json, index=[0])
            y = model.predict(df)
            predicts = pd.concat([predicts, pd.DataFrame.from_dict({"id": [form_json.get("id")], "result": [y[0]]})],
                                 ignore_index=True)

        # Save DataFrame to csv file
        target_path = path.join(path.dirname(path.dirname(path.abspath(__file__))),
                                'data', 'predictions', 'predictions.csv')
        predicts.to_csv(target_path, index=False)
        logging.info(f'Predictions are saved to {target_path}')
    except Exception as e:
        raise Exception(e)


if __name__ == '__main__':
    predict()


