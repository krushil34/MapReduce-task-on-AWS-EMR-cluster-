# This script Calculate the average tips to revenue ratio of the drivers for different pickup locations in sorted format.

from mrjob.job import MRJob

class AverageTipsToRevenueRatio(MRJob):

    def mapper(self, _, line):
        # Skip the header line
        if not line.startswith('VendorID'):
            fields = line.split(',')
            pickup_location = fields[7] # Extract pickup location ID
            total_revenue = float(fields[16]) # Extract total revenue
            tips = float(fields[13]) # Extract tips amount
            yield pickup_location, (tips, total_revenue)

    def combiner(self, pickup_location, tips_revenues):
        # Aggregate tips and revenue locally before sending to reducer
        total_tips = 0
        total_revenue = 0
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        yield pickup_location, (total_tips, total_revenue)

    def reducer(self, pickup_location, tips_revenues):
        # Compute the average tips-to-revenue ratio for each pickup location
        total_tips = 0
        total_revenue = 0
        for tips, revenue in tips_revenues:
            total_tips += tips
            total_revenue += revenue
        average_tips_to_revenue_ratio = total_tips / total_revenue
        yield pickup_location, average_tips_to_revenue_ratio


if __name__ == '__main__':
    AverageTipsToRevenueRatio.run()