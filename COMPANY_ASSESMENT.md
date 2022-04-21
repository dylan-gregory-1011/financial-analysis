# Company Assesment Model for Financial Analysis.

## Factors to investigate:

1. Value: fair value of company shares, compared to other companies and the market
2. Future Performance: Anticipated future performance based on analyst's forecast
3. Past Performance: Historical Financial Performance
4. Health: Financial Strength of the companies balance sheet
5. Dividends: Dividend being paid

For each assesment criteria, there are 6 "checks" performed and if a check is succesful then it is assigned a score of 1


## Checks (Main):
1. Value

    - Discount to Intrinsic Value > 40%
    - Discount to Intrisic Value > 20%
    - To determine fair value, use Discounted Cash Flows (DCF = incoming cash flow - expenses)
        - if not financial stock: 2-Stage DCF -> High growth stage (ten years of levered free clash flow to equity. If no estimates are available, extrapolate using historical average growth rate) + stable growth stage (Gordon Growth formula is used with an assuption that the company will continue to grow its earnings at the 10-year government bond rate forever). the sum of the cash flow are then discounted to todays value using a discount rate and divided by shares on issue giving a Value Per share
        - elif Insurance or bank: Excess returns method = (Return on Equity - Cost of Equity) (Book Value of Equity), Terminal Value = Excess Return / (Cost of Equity - Expected Growth Rate), Book Value of Equity = equity capital invested in comp + retained earnings, Company Valuation = Book Value of Equity + Present Value of Terminal Value, Value Per Share = (Book Value of Equity + Present Value of Terminal Value) / Shares Outstanding, discount rate = Cost of Equity = Risk Free Rate + (Levered Beta \* Equity Risk Premium) * Risk Free Rate is the 10-year government bond rate and equity risk premium is 10% less the risk free rate
    - 0 < PE Ratio < Market
    - 0 < PE Ratio < Industry
    - Close / EPS
    - 0 < PEG Ratio < 1
    - PE Ratio / Annual Net Income Growth Rate
    -  0 < PB ratio < Industry
    - Close / Book Value Per Share :: BVPS = Total Equity - Preferred Equity / Total Shares Outstanding

2. Future Performance

    - Revenue Growth > 20% per year
    - Earnings growth > 20% per year
    - Revenue growth > Market
    - Earnings growth > Market
    - Earnings growth > CPI + Risk Free Investments
    - 3yr ROE > 20%
    - Net Income In 3 years / Average Equity

3. Past Performance:
    - ROE > 20% -> Net Income / Average Equity (Shareholders Funds)
    - ROA > Industry -> Net Income / Total Assets
    - ROCE > -3yr ROCE -> Return On Capital Employed = EBIT*tax rate / (Invested Capital)
    - EPS > -5yr EPS
    - EPS growth > Industry
    - EPS growth > 5yr annual average -> EPS = Net Profit /Average Number of Shares on Issue during the year

4. Health:
    - Short Term assets > Short Term liabilities
    - Short term assets > Long term liabilities
    - Interest on debt < 5x Earnings -> EBIT
    - debt < short term assets
    - debt / equity < -5yr d/e -> D/E = Total Debt / Total Book Value of Shareholders Equity
    - Debt/Equity < 40%
        

For companies that are loss making and loss making on average)

    - cash+shortterminvestments > stable negative free cash flow for more than 1 year
    - cash+shortterminvestments > growing negative free cash flow for more than 1 year
    - This covers whether the cash rate should grow or sink at the same rate annualy as it had the past 3 years

For Financial Institutions

    - Is Leverage (Assets to Equity) > 20x
    - Bad Loan Coverage > 100%
    - Deposits/Liabilities > 1/2
    - Loans/Assets > 110%
    - Loans/Deposits > 125%
    - Net Charge Off Ratio > 3%

5. Income:

    - 0< +3yr Payout Ratio < 90% -> Dividends Per Share / Earnings Per Share
    - Payout Ratio < 100%
    - 10yr DPS volatility < 25%
    - Div Yield > -5 yr Div Yield
    - Div Yield > Top 25%
    - Div Yield > 25% of Market
    - Div Yield > Cash Yield -> Annualized Dividend Paid / Share Price

6. Management:

    - CEO compensation compared to relative companies
    - CEO compensation increased more than 20% while EPS down by more than 20%
    - average tenure of the management team less than 2 years
    - Average tenure of the board of directors < 3 years
    - Company insiders net buyers or sellers
    - Enterprise Value: EV = Close*commonstocksharesoutstanding OR Close*commonstockvalue + (shorttermborrowings + commercialpaper + minorityinterest) + (longtermdebtcurrent + longtermdebtnoncurrent + duetoaffiliatenoncurrent) OR (longtermdebtandcapitalleaseobligations + longtermdebtandcapitalleaseobligationscurrent) - (cashandcashequivalentsatcarryingvalue + restrictedcashandcashequivalentsatcarryingvalue) - (availableforsalesecuritiescurrent + marketablesecuritiescurrent)