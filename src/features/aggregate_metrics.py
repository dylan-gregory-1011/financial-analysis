##!/usr/bin/env python
"""
Industry Averages:
Industry averages are calculated by region on a weekly basis. The calculation for each data point is weighted by market cap.

"""

import pandas as pd

__author__ = "Dylan Smith"
__copyright__ = "Copyright (C) 2020 Dylan Smith"
__credits__ = ["Dylan Smith"]

__license__ = "Personal Use"
__version__ = "1.0"
__maintainer__ = "Dylan Smith"
__email__ = "-"
__status__ = "Development"

class AggregateCompanyMetrics(object):
    def __init__(self, sql_analytics, sql_mstr):
        """ Mongo Client used to connect to the MongoDB
        """
        self.analytics = sql_analytics
        self.sql_mstr = sql_mstr

    def aggregateValue(comp_data):
        """ Calculate the aggregate value for all of the value of the company
        """
        score = 0
        if dcf <= 0.2 * close:
            """ Compares the fair value (ie the calculated DCF value) to the current share price.
                If the share price is => 20% below the fair value, it is considered to be moderately
                undervalued and is scored one point.
            """
            score += 1
        elif dcf <= 0.4 * close:
            """ If the share price is below the fair value by 40% or more, then it is considered
                substantially undervalued and is scored one point.
            """
            score += 1
        elif pe < mkt_avg and pe > 0:
            """ The PE ratio is compared to the whole market PE ratio for the country of listing.
            """
            score += 1
        elif pe < ind_avg and pe > 0:
            """ The PE ratio is compared to the industry average PE ratio for its industry classification.
            """
            score += 1
        elif peg <= 1 and peg >= 0:
            """ The PEG ratio is compared to the range of 0 to 1.
            """
            score += 1
        elif pb >= 0 and pb <= 1:
            """ The PB ratio is compared to the relevant industry average.
            """
            score += 1

    def aggregatePastPerformance(comp_data):
        """ Calculate the aggregate value for all of the value of the company
        """
        score = 0
        if eps_growth > ind_avg_eps_growth:
            """ If the EPS growth of the company is > the EPS growth of its relevant
                industry average the stock is scored one point.
            """
            score += 1
        elif eps > eps_lag5:
            """ If the EPS for the current year is > the EPS from 5 years ago the stock is scored one point.
            """
            score += 1
        elif eps_growth > eps_avg_growth:
            """ If current year growth in EPS is > the average annual growth in EPS over the
                past 5 years the stock is scored one point.
            """
            score += 1
        elif roe >= 0.2:
            """ If the ROE for the company for the current year is > 20% the stock is scored one point.
            """
            score += 1
        elif roce > roce_3yr_ago:
            """ If the current year ROCE is > the ROCE from 3 years ago the stock is scored one point.
            """
            score += 1
        elif roa > ind_avg_roa:
            """ If the current year ROA is > the relevant industry average the stock is scored one point.
            """
            score += 1

    def aggregateHealth(comp_data):
        """ Calculate the aggregate value for all of the value of the company
        """
        score = 0
        if shrt_assets > shrt_liabs:
            """ This check measures whether, on a short term basis (< 12 months), the company has a net
                positive financial position. In the event of financial stress, this check indicates whether
                the company could liquidate short term assets to meet its short term liabilities.
            """
            score += 1
        elif shrt_assets > long_liabs:
            """ This check measures whether the company holds short term assets which are greater than its long
                term (>12 months) liabilities. In the event of financial stress, this check indicates whether the
                company could realise short term assets to meet its long term liabilities.
            """
            score += 1
        elif debt_to_equity < debt_to_equity_five:
            """ The Debt to Equity ratio for the current year is compared to the debt to equity ratio 5 years ago.
                If the ratio has not increased, or has fallen, the stock is scored one point.
            """
            score += 1
        elif debt_to_equity <= 0.4:
            """ If Debt to Equity ratio is < 40% the stock is scored one point.
            """
            score += 1
        elif debt_to_cash_flow <= 0.5:
            """ This check indicates whether, in the event of financial stress, the company is able to meet its debt
                obligations using purely its cash flow for the year from its operational activities.
            """
            score += 1
        elif ebit >= 5*debt_interest:
            """ This check indicates whether the company's interest obligations are met through earnings before interest and tax
                (EBIT). A ratio of 5 times earnings indicates a strong level of coverage.
            """
            score += 1

        ######
        # Loss Making Companies
        #######
        if cash > stable_cash_burn:
            """ This check indicates whether the company’s cash and other liquid asset levels are high enough to cover its negative
                free cash flow over the next year, should the rate remain stable.
            """
            score += 1
        elif cash > growing_cash_burn:
            """ This check indicates whether the company’s cash and other liquid asset levels are high enough to cover its negative
                free cash flow over the next year, should rate grow or shrink at the same rate annually as it had in the past three
                years.
            """
            score += 1

    def aggregateFinanceIncome(comp_data):

        score = 0
        if assets < 20 * shareholders_equity:
            """ Leverage (or gearing) refers to the amount of assets held in a business when compared to the company's own resources
                (shareholders funds or equity).
                While Financial Institutions will always have an elevated level of leverage, if the level becomes too high the
                institution may come under stress in the event of adverse circumstances. A leverage ratio of 20 times or less is
                considered acceptable.
            """
            score +=1
        elif bad_loans_coverage > 1:
            """ Bad loans are loans made by Financial Institutions which are considered to be unrecoverable. Bad loan coverage
                refers to provisions set aside against potential bad loans. A provision of this nature is offset against profits.
                Bad loan provisions held are compared to the level of Bad loans actually written off. If the Bad Loan provisions
                are > actual Bad Debts written off the stock is scored one point.
            """
            score += 1
        elif total_deposits > 0.5 * total_liabilities:
            """ Financial Institutions borrow money (to lend) in many different forms. Deposits from customers generally bear
                the lowest risk as they are less volatile than other forms of borrowing in terms of both the amount available
                (which does not usually change quickly) and interest rate paid (the rates paid are set by the Financial Institution
                itself). Broadly the higher the level of Deposits held the less risky the Financial Institution is considered to be.
            """
            score += 1
        elif net_loans < 1.1 * total_assets:
            """ The Loans to Assets ratio measures the net loans outstanding as a percentage of total assets. The higher this
                ratio indicates a bank has a high level of loans and therefore its liquidity is low. The higher the ratio, the
                more risky a bank may be to higher defaults.

                Financial firms such as Banks with high Loans to Assets ratios rely on interest income from loans and other
                securities for a high portion of their total revenue. Those with lower ratios have more diversified sources of
                revenue, for example from investment banking and asset management.
            """
            score += 1
        elif total_loans < 1.25 * deposits:
            """ The Loans to Deposits (LTD) ratio measures the liquidity of a Financial Institution. Liquidity refers to
                the funds available to a Financial Institution to repay liabilities, in particular Deposits (recognising
                that Deposits must generally be repaid on demand, or at short notice). Loan assets on the other hand generally
                have a fixed term, and often cannot be readily realised.

                If the LTD ratio is too high, the Financial Institution may experience difficulty repaying Deposits if
                unusual circumstances arise. The observed LTD range is 50% (very liquid) to 175% (illiquid).
            """
            score += 1
        elif bad_loans < 0.3 * total_loans:
            """ The level of Bad Loans incurred by a Financial Institution is a key indicator of the quality of loans made. A
                high level of Bad Loans may indicate that the Financial Institution is engaged in overly risky lending practices.
            """
            score += 1
    def aggregateIncome(comp_data):
        """ Calculate the aggregate Income
        """
        score = 0
        if div_ratio > mkt_25th_percentile:
            """ If the Dividend Yield is > the 25th percentile of the company’s market the stock is scored one point.
            """
            score += 1
        elif div_ratio > mkt_25th_percentile:
            """ If the Dividend Yield is > the 75th percentile of the company’s market the stock is scored one point.
            """
            score += 1
        elif max_drop >= 0.1 *div_yield:
            """ To check for volatility SWS looks at the historical dividend per share payments, if at any point in the
                last 10 years a drop of greater than 10% has occurred the dividend is considered volatile. Note this check is
                based on the per share dividend amounts paid, not the yield.

                One point is scored if there have been no annual drops in DPS of more than 10% in the past 10 years. This check
                also fails by default if the stock has been paying a dividend for less than 10 years.
            """
            score += 1
        elif payout_ratio >= 0 and payout_ratio <= 0.9:
            """ The current annualised dividend amount paid is compared to the annualised dividend amount paid 10 years ago.
                This check also fails by default if the stock has been paying a dividend for less than 10 years.
            """
            score += 1
        elif debt_to_cash_flow <= 0.5:
            """ The payout ratio (as defined below) is used to check if the dividend is affordable by the company.
            """
            score += 1
        elif ebit >= 5*debt_interest:
            """ To check for future dividend coverage the payout ratio in 3 years’ time is estimated. This is done using
                consensus analyst estimates for Dividend per share payments and Earnings per share.

                If the estimated payout ratio in 3 years is greater than 0% and less than 90% the stock is scored one point,
                in the case of REITs this threshold is 100%.
            """
            score += 1
    def aggregateManagement(comp_data):
        """ Calculate the aggregate Income
        """
        score = 0
        if salary < 0.3 * median_sal:
            """ We compare each company with a group of companies of similar size, and then test how the CEO is paid
                compared to the median of that group. If the CEO is paid 30% or more less than the median, then we
                consider pay to be below average. If the CEO is paid within 30% of the median pay, then we consider
                the pay to be around average. If the CEO is paid 30% or more above the median pay, then we consider
                the pay to be above average.
            """
            score += 1
        elif salary_increase > 0.2 and eps_change > -0.2:
            """ The increase in CEO compensation over the most recent financial period is compared to the movement in
                company earnings per share (EPS) over the most recent financial period.

                If CEO compensation has increased more than 20%, and the company's EPS has fallen more than 20%,
                this check is flagged.
            """
            score += 1
        elif avg_turnout < 2:
            """ If the average tenure of the management team on average is < 2 years this check is flagged.
            """
            score += 1
        elif avg_turn_board < 3:
            """ If the average tenure of the Board of Directors is < 3 years this check is flagged.
            """
            score += 1
