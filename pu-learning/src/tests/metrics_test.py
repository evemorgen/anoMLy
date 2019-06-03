import sqlite3
import json
import pandas
import numpy
import time

from pandas import DataFrame
from numpy import nan, concatenate, array
from more_itertools import chunked
from sklearn.model_selection import train_test_split
from ..puLearning.puAdapter import PUAdapter
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import precision_recall_fscore_support
import tsfresh.feature_extraction.feature_calculators as feature


def extend(tup, longest):
    feature, pu_value = tup
    no_nones = [x for x in feature if x is not None]
    to_extend = longest - len(no_nones)
    #if pu_value > 0:
    #    print(pu_value)
    return concatenate([no_nones, array([0 for _ in range(to_extend)])]), pu_value
    #feature_array = array([v for v in feature if v is not None])
    #return feature_array.mean(), pu_value


def extract_features(data):
    day = 24 * 60
    
    return list(numpy.nan_to_num(numpy.array([
        feature.symmetry_looking(data, [{'r': 0.3}])[0][1],
        feature.variance_larger_than_standard_deviation(data).bool(),
        feature.ratio_beyond_r_sigma(data, 2),
        feature.has_duplicate_max(data),
        feature.has_duplicate_min(data),
        feature.has_duplicate(data),
        feature.agg_autocorrelation(numpy.array(data.value), [{'f_agg': 'mean', 'maxlag': day}])[0][1],
        feature.partial_autocorrelation(data, [{'lag': day}])[0][1],
        feature.abs_energy(numpy.array(data.value)),
        feature.mean_change(data),
        feature.mean_second_derivative_central(data),
        feature.median(data),
        float(feature.mean(data)),
        float(feature.standard_deviation(data)),
        float(feature.longest_strike_below_mean(data)),
        float(feature.longest_strike_above_mean(data)),
        int(feature.number_peaks(data, 10)),
        feature.linear_trend(numpy.array(data.value), [{'attr': 'rvalue'}])[0][1],
        feature.c3(data, day),
        float(feature.maximum(data)),
        float(feature.minimum(data))
    ])))


"""
db = sqlite3.connect('/home/evemorgen/workspace/anoMLy/anomly/data/soia_email.db')
cursor = db.cursor()

pu_f1_scores = []
reg_f1_scores = []


cursor.execute('select start, end, metric_anomaly, metric_whole from soia_with_values;')
wholesome_df = DataFrame()
for start, end, m1, m2 in cursor.fetchall():
    anomaly = json.loads(m1)
    whole = json.loads(m2)

    anomaly_df = DataFrame(data=anomaly, columns=['value', 'timestamp'])
    anomaly_df.set_index('timestamp', inplace=True)
    if len(anomaly_df) == 0:
        continue
    anomaly_features = extract_features(anomaly_df) + [start, end]

    s = time.time()

    frames = []
    for _range in [(0, start), (end, len(whole))]:
        df = DataFrame(data=whole[_range[0]:_range[1]], columns=['value', 'timestamp'])
        df.set_index('timestamp', inplace=True)
        if len(df) > 0:
            df_features = extract_features(df) + [_range[0], _range[1]]
            frames.append(df_features)


    e = time.time()
    print("done, took %s" % (e - s))

    if len(anomaly) > 0:
        positive = DataFrame(data={'feature': [numpy.array(anomaly_features)], 'pu_value': 1})
        wholesome_df = pandas.concat([wholesome_df, positive])
        #chunks = [{'feature': [value for value, _ in values], 'pu_value': -1} for values in chunked(whole, len(anomaly))]
        #unlabeled = DataFrame(data=chunks)
        for frame in frames:
            unlabeled = DataFrame(data={'feature': [numpy.array(frame)], 'pu_value': -1})
            #positive = pandas.concat([positive, unlabeled])
            wholesome_df = pandas.concat([wholesome_df, unlabeled])

#to_extend = max([len(val) for val in wholesome_df['feature']])
#wholesome_df = wholesome_df.apply(lambda x: extend(x, to_extend), axis=1, result_type='expand')
#wholesome_df.columns = ['feature', 'pu_value']
#print([len(val) for val in wholesome_df['feature']])
#print(x_train, y_train)

#print(y for y in y_train if y != -1)
"""
"""
estimator.fit([[x] for x in x_train.values], [[y] for y in y_train])
y_pred = estimator.predict([[x] for x in x_test])
precision, recall, f1_score, _ = precision_recall_fscore_support(y_test, y_pred)
reg_f1_scores.append(f1_score[1])
print("F1 score: ", f1_score[1])
print("Precision: ", precision[1])
print("Recall: ", recall[1])
print()
print()
"""

#wholesome_df.to_pickle('wholesome.pikle')
wholesome_df = pandas.read_pickle('wholesome.pikle')
print(wholesome_df)

estimator = RandomForestClassifier(n_estimators=100,
                                           criterion='gini', 
                                           bootstrap=True,
                                           n_jobs=1)
pu_estimator = PUAdapter(estimator, hold_out_ratio=0.001)
pu_f1_scores = []
for i in range(100):
    x_train, x_test, y_train, y_test = train_test_split(wholesome_df['feature'], wholesome_df['pu_value'], test_size=0.25, random_state=42)
    #import pdb; pdb.set_trace()
    pu_estimator.fit([array(x) for x in x_train.values], array(y_train))
    y_pred = pu_estimator.predict([array(x) for x in x_test])
    precision, recall, f1_score, _ = precision_recall_fscore_support(y_test, y_pred)
    pu_f1_scores.append(f1_score[1])
    print(i)
    print("F1 score: ", f1_score[1])
    print("Precision: ", precision[1])
    print("Recall: ", recall[1])

cursor.close()
db.close()