# Outbrain click prediction: a Kaggle competition
## About
The [Outbrain Click Prediction](https://www.kaggle.com/c/outbrain-click-prediction)
was hosted on Kaggle. It was a machine learning competition with a large dataset (~30GB)
and with the objective to predict which advertisement would users click when
visiting a page.

## Approach
I decided to use the [libffm](https://github.com/guestwalk/libffm) to train 
the model, which is a library for field-aware-factorization machines. `libffm`
requires a special format for the input data, so I used `Python` scripts to transform
the data appropriately. The following scripts are included in this repo:
* `methods.py` contains the metrics of the competition (MAPK@7).
* `prepare_data_for_ffm.py` transforms the data in the suitable format.
* `extract_leak.py` extracts leaked information from the dataset about the 
response.
* `evaluate_test_data.py` computes the metric for a local validation test set.
* `create_submission_file_from_output.py` takes the `libffm` output and
transforms it into a suitable format for submission.

By using a the raw features and with suitable feature engineering I was
able to eventually rank 25th amongst 979 teams.
