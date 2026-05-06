import importlib.util
import unittest


@unittest.skipIf(importlib.util.find_spec("pyspark") is None, "pyspark is not installed")
class SparkJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from processing.spark_utils import get_spark

        cls.spark = get_spark("subway-dash-unit-tests")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_clean_weather_daily_bucket(self):
        from processing.clean_weather import clean_weather_dataframe

        df = self.spark.createDataFrame(
            [
                {"DATE": "2024-01-01", "PRCP": "0.00", "SNOW": "0.00", "TMAX": "45", "TMIN": "35"},
                {"DATE": "2024-01-02", "PRCP": "0.25", "SNOW": "0.00", "TMAX": "42", "TMIN": "33"},
                {"DATE": "2024-01-03", "PRCP": "0.05", "SNOW": "0.30", "TMAX": "30", "TMIN": "20"},
            ]
        )
        rows = {str(row.date): row.weather_bucket for row in clean_weather_dataframe(df).collect()}
        self.assertEqual(rows["2024-01-01"], "clear")
        self.assertEqual(rows["2024-01-02"], "rain")
        self.assertEqual(rows["2024-01-03"], "snow")

    def test_baseline_aggregation(self):
        from processing.build_baseline import build_baseline_dataframes

        ridership = self.spark.createDataFrame(
            [
                {
                    "station_complex_id": "613",
                    "entries": 100.0,
                    "transit_timestamp": "2024-01-03 08:00:00",
                    "date": "2024-01-03",
                    "hour_of_day": 8,
                    "day_of_week": 4,
                    "weather_bucket": "clear",
                },
                {
                    "station_complex_id": "613",
                    "entries": 200.0,
                    "transit_timestamp": "2024-01-10 08:00:00",
                    "date": "2024-01-10",
                    "hour_of_day": 8,
                    "day_of_week": 4,
                    "weather_bucket": "clear",
                },
            ]
        )
        baseline, station_max = build_baseline_dataframes(ridership, lookback_days=0)
        self.assertEqual(baseline.count(), 1)
        self.assertEqual(station_max.collect()[0].max_hourly_entries, 200.0)

    def test_speed_layer_join_uses_event_timestamp(self):
        from processing.speed_layer_join import build_delay_stream

        vehicles = self.spark.createDataFrame(
            [
                {
                    "trip_id": "trip-1",
                    "stop_id": "A01N",
                    "station_complex_id": "611",
                    "event_timestamp": "2026-05-05 08:00:00",
                },
                {
                    "trip_id": "trip-2",
                    "stop_id": "A02N",
                    "station_complex_id": None,
                    "event_timestamp": "2026-05-05 08:00:00",
                },
            ]
        )
        delays = self.spark.createDataFrame(
            [
                {
                    "trip_id": "trip-1",
                    "stop_id": "A01N",
                    "arrival_delay_secs": 90,
                    "event_timestamp": "2026-05-05 08:01:00",
                },
                {
                    "trip_id": "trip-1",
                    "stop_id": "A01N",
                    "arrival_delay_secs": 30,
                    "event_timestamp": "2026-05-05 08:04:00",
                },
            ]
        )

        result = build_delay_stream(vehicles, delays)
        rows = result.collect()
        self.assertGreater(len(rows), 0)
        self.assertEqual({row.station_complex_id for row in rows}, {"611"})
        self.assertEqual({row.avg_arrival_delay_secs for row in rows}, {60.0})


if __name__ == "__main__":
    unittest.main()
