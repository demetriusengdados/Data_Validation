# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for feature_skew_detector."""

import traceback

from absl.testing import absltest
import apache_beam as beam
from apache_beam.testing import util
import tensorflow as tf
from tensorflow_data_validation.skew import feature_skew_detector
from tensorflow_data_validation.skew.protos import feature_skew_results_pb2
from tensorflow_data_validation.utils import test_util

from google.protobuf import text_format

# Ranges of values for identifier features.
_IDENTIFIER_RANGE = 2
# Names of identifier features.
_IDENTIFIER1 = 'id1'
_IDENTIFIER2 = 'id2'
# Name of feature that is skewed in the test data.
_SKEW_FEATURE = 'skewed'
# Name of feature that appears only in the training data and not serving.
_TRAINING_ONLY_FEATURE = 'training_only'
# Name of feature that appears only in the serving data and not training.
_SERVING_ONLY_FEATURE = 'serving_only'
# Name of feature that has the same value in both training and serving data.
_NO_SKEW_FEATURE = 'no_skew'
# Name of feature that has skew but should be ignored.
_IGNORE_FEATURE = 'ignore'
# Name of float feature that has values that are close in training and serving
# data.
_CLOSE_FLOAT_FEATURE = 'close_float'


def make_sample_equal_fn(test, expected_size, potential_samples):
  """Makes a matcher function for checking SkewPair results."""

  def _matcher(actual):
    try:
      test.assertLen(actual, expected_size)
      for each in actual:
        test.assertTrue(each in potential_samples)
    except AssertionError:
      raise util.BeamAssertException(traceback.format_exc())

  return _matcher


def get_test_input(include_skewed_features, include_close_floats):
  training_examples = list()
  serving_examples = list()
  skew_pairs = list()
  for i in range(_IDENTIFIER_RANGE):
    for j in range(_IDENTIFIER_RANGE):
      base_example = tf.train.Example()
      base_example.features.feature[_IDENTIFIER1].int64_list.value.append(i)
      base_example.features.feature[_IDENTIFIER2].int64_list.value.append(j)
      base_example.features.feature[_NO_SKEW_FEATURE].int64_list.value.append(1)

      training_example = tf.train.Example()
      training_example.CopyFrom(base_example)
      serving_example = tf.train.Example()
      serving_example.CopyFrom(base_example)

      training_example.features.feature[
          _IGNORE_FEATURE].int64_list.value.append(0)
      serving_example.features.feature[_IGNORE_FEATURE].int64_list.value.append(
          1)

    if include_close_floats:
      training_example.features.feature[
          _CLOSE_FLOAT_FEATURE].float_list.value.append(1.12345)
      serving_example.features.feature[
          _CLOSE_FLOAT_FEATURE].float_list.value.append(1.12456)

    if include_skewed_features:
      # Add three different kinds of skew: value mismatch, appears only in
      # training, and appears only in serving.
      training_example.features.feature[_SKEW_FEATURE].int64_list.value.append(
          0)
      serving_example.features.feature[_SKEW_FEATURE].int64_list.value.append(1)
      training_example.features.feature[
          _TRAINING_ONLY_FEATURE].int64_list.value.append(0)
      serving_example.features.feature[
          _SERVING_ONLY_FEATURE].int64_list.value.append(1)

      skew_pair = feature_skew_results_pb2.SkewPair()
      skew_pair.training.CopyFrom(training_example)
      skew_pair.serving.CopyFrom(serving_example)
      skew_pair.matched_features.append(_NO_SKEW_FEATURE)
      skew_pair.mismatched_features.append(_SKEW_FEATURE)
      skew_pair.training_only_features.append(_TRAINING_ONLY_FEATURE)
      skew_pair.serving_only_features.append(_SERVING_ONLY_FEATURE)
      skew_pairs.append(skew_pair)

    training_examples.append(training_example)
    serving_examples.append(serving_example)
  return (training_examples, serving_examples, skew_pairs)


class FeatureSkewDetectorTest(absltest.TestCase):

  def test_detect_feature_skew(self):
    training_examples, serving_examples, _ = get_test_input(
        include_skewed_features=True, include_close_floats=True)

    expected_result = [
        text_format.Parse(
            """
        feature_name: 'close_float'
        training_count: 2
        serving_count: 2
        mismatch_count: 2
        diff_count: 2""", feature_skew_results_pb2.FeatureSkew()),
        text_format.Parse(
            """
        feature_name: 'skewed'
        training_count: 2
        serving_count: 2
        mismatch_count: 2
        diff_count: 2""", feature_skew_results_pb2.FeatureSkew()),
        text_format.Parse(
            """
        feature_name: 'training_only'
        training_count: 2
        training_only: 2
        diff_count: 2""", feature_skew_results_pb2.FeatureSkew()),
        text_format.Parse(
            """
        feature_name: 'serving_only'
        serving_count: 2
        serving_only: 2
        diff_count: 2""", feature_skew_results_pb2.FeatureSkew()),
        text_format.Parse(
            """
        feature_name: 'no_skew'
        training_count: 2
        serving_count: 2
        match_count: 2
        diff_count: 0""", feature_skew_results_pb2.FeatureSkew()),
    ]

    with beam.Pipeline() as p:
      training_examples = p | 'Create Training' >> beam.Create(
          training_examples)
      serving_examples = p | 'Create Serving' >> beam.Create(serving_examples)
      skew_result, _ = ((training_examples, serving_examples)
                        | feature_skew_detector.DetectFeatureSkewImpl(
                            [_IDENTIFIER1, _IDENTIFIER2], [_IGNORE_FEATURE]))
      util.assert_that(
          skew_result,
          test_util.make_skew_result_equal_fn(self, expected_result))

  def test_detect_no_skew(self):
    training_examples, serving_examples, _ = get_test_input(
        include_skewed_features=False, include_close_floats=False)

    expected_result = [
        text_format.Parse(
            """
        feature_name: 'no_skew'
        training_count: 2
        serving_count: 2
        match_count: 2
        diff_count: 0""", feature_skew_results_pb2.FeatureSkew()),
    ]

    with beam.Pipeline() as p:
      training_examples = p | 'Create Training' >> beam.Create(
          training_examples)
      serving_examples = p | 'Create Serving' >> beam.Create(serving_examples)
      skew_result, skew_sample = (
          (training_examples, serving_examples)
          | feature_skew_detector.DetectFeatureSkewImpl(
              [_IDENTIFIER1, _IDENTIFIER2], [_IGNORE_FEATURE], sample_size=2))
      util.assert_that(
          skew_result,
          test_util.make_skew_result_equal_fn(self, expected_result),
          'CheckSkewResult')
      util.assert_that(skew_sample, make_sample_equal_fn(self, 0, []),
                       'CheckSkewSample')

  def test_obtain_skew_sample(self):
    training_examples, serving_examples, skew_pairs = get_test_input(
        include_skewed_features=True, include_close_floats=False)

    sample_size = 1
    potential_samples = skew_pairs
    with beam.Pipeline() as p:
      training_examples = p | 'Create Training' >> beam.Create(
          training_examples)
      serving_examples = p | 'Create Serving' >> beam.Create(serving_examples)
      _, skew_sample = (
          (training_examples, serving_examples)
          | feature_skew_detector.DetectFeatureSkewImpl(
              [_IDENTIFIER1, _IDENTIFIER2], [_IGNORE_FEATURE], sample_size))
      util.assert_that(
          skew_sample, make_sample_equal_fn(self, sample_size,
                                            potential_samples))

  def test_empty_inputs(self):
    training_examples, serving_examples, _ = get_test_input(
        include_skewed_features=True, include_close_floats=True)

    # Expect no skew results or sample in each case.
    expected_result = list()

    # Empty training collection.
    with beam.Pipeline() as p:
      training_examples_1 = p | 'Create Training' >> beam.Create([])
      serving_examples_1 = p | 'Create Serving' >> beam.Create(serving_examples)
      skew_result_1, skew_sample_1 = (
          (training_examples_1, serving_examples_1)
          | feature_skew_detector.DetectFeatureSkewImpl(
              [_IDENTIFIER1, _IDENTIFIER2], [_IGNORE_FEATURE], sample_size=1))
      util.assert_that(
          skew_result_1,
          test_util.make_skew_result_equal_fn(self, expected_result),
          'CheckSkewResult')
      util.assert_that(skew_sample_1,
                       make_sample_equal_fn(self, 0, expected_result),
                       'CheckSkewSample')

    # Empty serving collection.
    with beam.Pipeline() as p:
      training_examples_2 = p | 'Create Training' >> beam.Create(
          training_examples)
      serving_examples_2 = p | 'Create Serving' >> beam.Create([])
      skew_result_2, skew_sample_2 = (
          (training_examples_2, serving_examples_2)
          | feature_skew_detector.DetectFeatureSkewImpl(
              [_IDENTIFIER1, _IDENTIFIER2], [_IGNORE_FEATURE], sample_size=1))
      util.assert_that(
          skew_result_2,
          test_util.make_skew_result_equal_fn(self, expected_result),
          'CheckSkewResult')
      util.assert_that(skew_sample_2,
                       make_sample_equal_fn(self, 0, expected_result),
                       'CheckSkewSample')

    # Empty training and serving collections.
    with beam.Pipeline() as p:
      training_examples_3 = p | 'Create Training' >> beam.Create([])
      serving_examples_3 = p | 'Create Serving' >> beam.Create([])
      skew_result_3, skew_sample_3 = (
          (training_examples_3, serving_examples_3)
          | feature_skew_detector.DetectFeatureSkewImpl(
              [_IDENTIFIER1, _IDENTIFIER2], [_IGNORE_FEATURE], sample_size=1))
      util.assert_that(
          skew_result_3,
          test_util.make_skew_result_equal_fn(self, expected_result),
          'CheckSkewResult')
      util.assert_that(skew_sample_3,
                       make_sample_equal_fn(self, 0, expected_result),
                       'CheckSkewSample')

  def test_float_precision_configuration(self):
    training_examples, serving_examples, _ = get_test_input(
        include_skewed_features=True, include_close_floats=True)

    expected_result = [
        text_format.Parse(
            """
        feature_name: 'skewed'
        training_count: 2
        serving_count: 2
        mismatch_count: 2
        diff_count: 2""", feature_skew_results_pb2.FeatureSkew()),
        text_format.Parse(
            """
        feature_name: 'training_only'
        training_count: 2
        training_only: 2
        diff_count: 2""", feature_skew_results_pb2.FeatureSkew()),
        text_format.Parse(
            """
        feature_name: 'serving_only'
        serving_count: 2
        serving_only: 2
        diff_count: 2""", feature_skew_results_pb2.FeatureSkew()),
        text_format.Parse(
            """
        feature_name: 'no_skew'
        training_count: 2
        serving_count: 2
        match_count: 2""", feature_skew_results_pb2.FeatureSkew()),
    ]

    expected_with_float = expected_result + [
        text_format.Parse(
            """
        feature_name: 'close_float'
        training_count: 2
        serving_count: 2
        mismatch_count: 2
        diff_count: 2""", feature_skew_results_pb2.FeatureSkew())
    ]

    # Do not set a float_round_ndigits.
    with beam.Pipeline() as p:
      training_examples_1 = p | 'Create Training' >> beam.Create(
          training_examples)
      serving_examples_1 = p | 'Create Serving' >> beam.Create(serving_examples)
      skew_result, _ = ((training_examples_1, serving_examples_1)
                        | feature_skew_detector.DetectFeatureSkewImpl(
                            [_IDENTIFIER1, _IDENTIFIER2], [_IGNORE_FEATURE],
                            sample_size=1))
      util.assert_that(
          skew_result,
          test_util.make_skew_result_equal_fn(self, expected_with_float))

    expected_with_float_and_option = expected_result + [
        text_format.Parse(
            """
              feature_name: 'close_float'
              training_count: 2
              serving_count: 2
              match_count: 2
              """, feature_skew_results_pb2.FeatureSkew())
    ]

    # Set float_round_ndigits
    with beam.Pipeline() as p:
      training_examples_2 = p | 'Create Training' >> beam.Create(
          training_examples)
      serving_examples_2 = p | 'Create Serving' >> beam.Create(serving_examples)
      skew_result, _ = ((training_examples_2, serving_examples_2)
                        | feature_skew_detector.DetectFeatureSkewImpl(
                            [_IDENTIFIER1, _IDENTIFIER2], [_IGNORE_FEATURE],
                            sample_size=1,
                            float_round_ndigits=2))
      util.assert_that(
          skew_result,
          test_util.make_skew_result_equal_fn(self,
                                              expected_with_float_and_option))

  def test_no_identifier_features(self):
    training_examples, serving_examples, _ = get_test_input(
        include_skewed_features=False, include_close_floats=False)
    with self.assertRaisesRegex(ValueError,
                                'At least one feature name must be specified'):
      with beam.Pipeline() as p:
        training_examples = p | 'Create Training' >> beam.Create(
            training_examples)
        serving_examples = p | 'Create Serving' >> beam.Create(serving_examples)
        _ = ((training_examples, serving_examples)
             | feature_skew_detector.DetectFeatureSkewImpl([]))

  def test_duplicate_identifiers_allowed_with_duplicates(self):
    training_example_1 = text_format.Parse(
        """
        features {
          feature {
            key: "id"
            value { int64_list { value: 1 } }
          }
          feature {
            key: "val"
            value { int64_list { value: 100 } }
          }
        }
        """, tf.train.Example())
    training_example_2 = text_format.Parse(
        """
        features {
          feature {
            key: "id"
            value { int64_list { value: 1 } }
          }
          feature {
            key: "val"
            value { int64_list { value: 50 } }
          }
        }
        """, tf.train.Example())
    serving_example = text_format.Parse(
        """
        features {
          feature {
            key: "id"
            value { int64_list { value: 1 } }
          }
          feature {
            key: "val"
            value { int64_list { value: 100 } }
          }
          feature {
            key: "val2"
            value { int64_list { value: 100 } }
          }
        }
        """, tf.train.Example())
    expected_result = [
        text_format.Parse(
            """
        feature_name: 'val'
        training_count: 2
        serving_count: 2
        match_count: 1
        mismatch_count: 1
        diff_count: 1""", feature_skew_results_pb2.FeatureSkew()),
        text_format.Parse(
            """
        feature_name: 'val2'
        training_count: 0
        serving_count: 2
        serving_only: 2
        diff_count: 2""", feature_skew_results_pb2.FeatureSkew()),]
    with beam.Pipeline() as p:
      training_examples = p | 'Create Training' >> beam.Create(
          [training_example_1, training_example_2])
      serving_examples = p | 'Create Serving' >> beam.Create([serving_example])
      skew_result, _ = ((training_examples, serving_examples)
                        | feature_skew_detector.DetectFeatureSkewImpl(
                            ['id'], [], allow_duplicate_identifiers=True))
      util.assert_that(
          skew_result,
          test_util.make_skew_result_equal_fn(self, expected_result))

  def test_duplicate_identifiers_not_allowed_with_duplicates(self):
    training_example_1 = text_format.Parse(
        """
        features {
          feature {
            key: "id"
            value { int64_list { value: 1 } }
          }
          feature {
            key: "val"
            value { int64_list { value: 100 } }
          }
        }
        """, tf.train.Example())
    training_example_2 = text_format.Parse(
        """
        features {
          feature {
            key: "id"
            value { int64_list { value: 1 } }
          }
          feature {
            key: "val"
            value { int64_list { value: 50 } }
          }
        }
        """, tf.train.Example())
    serving_example = text_format.Parse(
        """
        features {
          feature {
            key: "id"
            value { int64_list { value: 1 } }
          }
          feature {
            key: "val"
            value { int64_list { value: 100 } }
          }
          feature {
            key: "val2"
            value { int64_list { value: 100 } }
          }
        }
        """, tf.train.Example())
    with beam.Pipeline() as p:
      training_examples = p | 'Create Training' >> beam.Create(
          [training_example_1, training_example_2])
      serving_examples = p | 'Create Serving' >> beam.Create([serving_example])
      skew_result, _ = ((training_examples, serving_examples)
                        | feature_skew_detector.DetectFeatureSkewImpl(
                            ['id'], [], allow_duplicate_identifiers=False))
      util.assert_that(
          skew_result,
          test_util.make_skew_result_equal_fn(self, []))

    runner = p.run()
    runner.wait_until_finish()
    result_metrics = runner.metrics()
    actual_counter = result_metrics.query(
        beam.metrics.metric.MetricsFilter().with_name(
            'skipped_duplicate_identifier'))['counters']
    self.assertLen(actual_counter, 1)
    self.assertEqual(actual_counter[0].committed, 1)

  def test_telemetry(self):
    base_example = tf.train.Example()
    base_example.features.feature[_IDENTIFIER1].int64_list.value.append(1)

    training_example = tf.train.Example()
    training_example.CopyFrom(base_example)
    serving_example = tf.train.Example()
    serving_example.CopyFrom(base_example)

    # Add Identifier 2 to training example only.
    training_example.features.feature[_IDENTIFIER2].int64_list.value.append(2)

    p = beam.Pipeline()
    training_data = p | 'Create Training' >> beam.Create([training_example])
    serving_data = p | 'Create Serving' >> beam.Create([serving_example])
    _, _ = ((training_data, serving_data)
            | feature_skew_detector.DetectFeatureSkewImpl(
                [_IDENTIFIER1, _IDENTIFIER2]))
    runner = p.run()
    runner.wait_until_finish()
    result_metrics = runner.metrics()

    # Serving example does not have Identifier 2.
    actual_counter = result_metrics.query(
        beam.metrics.metric.MetricsFilter().with_name(
            'examples_with_missing_identifier_features'))['counters']
    self.assertLen(actual_counter, 1)
    self.assertEqual(actual_counter[0].committed, 1)


if __name__ == '__main__':
  absltest.main()
