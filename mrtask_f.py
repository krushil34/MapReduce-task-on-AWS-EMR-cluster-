# This script analyzes How revenue vary over time by Calculating the average trip revenue per month - analysing it by hour of the day (day vs night) and the day of the week (weekday vs weekend).

from mrjob.job import MRJob
from datetime import datetime

class AverageRevenueOverTime(MRJob):

    def parse_datetime(self, datetime_str):
        # Function to parse date-time strings in multiple formats
        formats = ['%d-%m-%Y %H:%M:%S', '%d-%m-%Y %H:%M', '%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S']
        for fmt in formats:
            try:
                return datetime.strptime(datetime_str, fmt)
            except ValueError:
                pass
        raise ValueError('no valid date format found')


    def mapper(self, _, line):
        # Skip the header line
        if not line.startswith('VendorID'):
            fields = line.split(',')
            revenue = float(fields[16]) # Extract trip revenue
            pickup_datetime = self.parse_datetime(fields[1]) # Parse pickup datetime
            month = pickup_datetime.month # Extract the month
            hour = pickup_datetime.hour # Extract the hour of the day
            weekday = pickup_datetime.weekday() # Extract the day of the week 
            yield (month, hour, weekday), revenue

    def reducer(self, key, values):
        # Compute the average revenue per key (month, hour, weekday)
        total_revenue = 0
        num_trips = 0

        for revenue in values:
            total_revenue += revenue
            num_trips += 1

        average_revenue = total_revenue / num_trips

        yield key, average_revenue

if __name__ == '__main__':
    AverageRevenueOverTime.run()