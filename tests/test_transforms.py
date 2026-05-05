import unittest

from processing.transforms import (
    classify_alert_level,
    classify_weather,
    compute_congestion_score,
    compute_demand_intensity,
    compute_predicted_delay_mins,
    compute_service_deficit,
    haversine_meters,
    normalize_station_name,
    normalize_stop_id,
)


class TransformTests(unittest.TestCase):
    def test_normalize_stop_id_strips_direction(self):
        self.assertEqual(normalize_stop_id("127N"), "127")
        self.assertEqual(normalize_stop_id("127S"), "127")
        self.assertEqual(normalize_stop_id(" L06 "), "L06")

    def test_normalize_station_name(self):
        self.assertEqual(normalize_station_name("Times Sq-42 St"), "times sq 42 st")
        self.assertEqual(normalize_station_name("Jay St-MetroTech"), "jay st metrotech")

    def test_haversine_meters(self):
        self.assertLess(haversine_meters(40.7558, -73.9878, 40.7558, -73.9878), 0.1)
        self.assertGreater(haversine_meters(40.7558, -73.9878, 40.7527, -73.9772), 900)

    def test_weather_bucket(self):
        self.assertEqual(classify_weather(0.0, 0.0), "clear")
        self.assertEqual(classify_weather(0.2, 0.0), "rain")
        self.assertEqual(classify_weather(0.2, 0.2), "snow")

    def test_service_deficit(self):
        self.assertEqual(compute_service_deficit(30), 0.0)
        self.assertAlmostEqual(compute_service_deficit(180), 0.5)
        self.assertEqual(compute_service_deficit(600), 1.0)

    def test_demand_and_score(self):
        self.assertEqual(compute_demand_intensity(50, 0), 0.0)
        self.assertAlmostEqual(compute_demand_intensity(50, 100), 0.5)
        self.assertAlmostEqual(compute_congestion_score(0.6, 0.5), 0.3)

    def test_predicted_delay_and_alert(self):
        self.assertAlmostEqual(compute_predicted_delay_mins(180, 0.5), 4.5)
        self.assertEqual(classify_alert_level(0.1), "NORMAL")
        self.assertEqual(classify_alert_level(0.2), "MODERATE")
        self.assertEqual(classify_alert_level(0.5), "SEVERE")


if __name__ == "__main__":
    unittest.main()
