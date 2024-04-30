from dateutil.relativedelta import relativedelta
import numpy as np
import datetime as dt
import pandas as pd



address = "C:\\Users\\HWPS\\Desktop\\Quant\\심리지수\\CNN Fear And Greed KOR_For Git.xlsx"


# get dates which forms are as timestamp from xlsx.
def pulling_dates_up(start: str, end: str, country='US') -> list:  # start, end format are 'yyyy-mm'
    """
    pulling date data from excel files
    :param start: data from, "YYYY-mm-dd"
    :param end: data to, "YYYY-mm-dd"
    :param country: US or KOR
    :return: list of Dates
    :rtype: list
    """
    dataset = pd.read_excel(io=address, sheet_name="Main Index", usecols=[0, 1])  # US: S&P500, KOR: KOSPI200
    dataset.columns = ['Date', 'Price']
    dataset.set_index("Date", inplace=True)

    start = dt.datetime.strptime(start, "%Y-%m-%d")
    end = dt.datetime.strptime(end, "%Y-%m-%d")
    result = list(filter(lambda d: start <= d <= end and pd.notna(d), dataset.index))
    return result


def kospi_stock_market_tech(start_date, end_date):  # O(n^4)
    """
    For calculating the number of Listed, 52weeks High, 52weeks Low, Advancing(Declining) Equities Volumes
    A excel file will be exported and tt is only for KOR data. US data is not neeeded bacause if is from Bloomberg
    :param start_date: date start
    :param end_date: data end
    """
    date_list = pulling_dates_up(start_date, end_date, country='KOR')
    all_date_list = pulling_dates_up("1900-01-01", "2999-12-31", country='KOR')  # All days
    dataset_dict = {}
    input_dataset = {"trade": "Is Active", "price": "Last Price Adjusted", "volume": "Trading Value"}
    for pair in zip(input_dataset.keys(), input_dataset.values()):
        temporary_dataset = pd.read_excel(address, sheet_name=pair[1])
        temporary_dataset.set_index("Date", inplace=True)
        dataset_dict["dataset_" + pair[0]] = temporary_dataset

    num_list = []
    high_num_list = []
    low_num_list = []
    advancing_list = []
    declining_list = []
    for i in date_list:
        num = 0
        high_num = 0
        low_num = 0
        advancing = 0
        declining = 0
        if i in dataset_dict["dataset_price"].index:
            one_year_before_date = i - relativedelta(weeks=52)
            one_year_before_date_adjusted = max(list(filter(lambda x: x <= one_year_before_date, dataset_dict["dataset_price"].index)))  # when not business day
            yesterday = all_date_list[all_date_list.index(i) + 1]
            for j in dataset_dict["dataset_price"].columns:
                test = [i for i in dataset_dict["dataset_trade"].loc[i:one_year_before_date_adjusted, j]]
                if (1 not in test) and (dataset_dict["dataset_trade"].loc[i:one_year_before_date_adjusted, j].isnull().sum() == 0):  # Active in 1 year
                    # 52 Weeks High and Low
                    num = num + 1
                    if dataset_dict["dataset_price"].loc[i, j] == max(dataset_dict["dataset_price"].loc[i:one_year_before_date_adjusted, j]):
                        high_num = high_num + 1
                    else:
                        pass
                    if dataset_dict["dataset_price"].loc[i, j] == min(dataset_dict["dataset_price"].loc[i:one_year_before_date_adjusted, j]):
                        low_num = low_num + 1
                    else:
                        pass

                    # Advancing_Declining
                    if i in dataset_dict["dataset_volume"].index:
                        one_day_return = dataset_dict["dataset_price"].loc[i, j] - dataset_dict["dataset_price"].loc[yesterday, j]
                        if one_day_return > 0:
                            advancing = advancing + dataset_dict["dataset_volume"].loc[i, j]
                        elif one_day_return < 0:
                            declining = declining + dataset_dict["dataset_volume"].loc[i, j]
                        else:
                            pass
                    else:
                        advancing = np.nan
                        declining = np.nan
                else:
                    pass
            num_list.append(num)
            high_num_list.append(high_num)
            low_num_list.append(low_num)
            advancing_list.append(advancing)
            declining_list.append(declining)
        else:
            num_list.append(np.nan)
            high_num_list.append(np.nan)
            low_num_list.append(np.nan)
            advancing_list.append(np.nan)
            declining_list.append(np.nan)

    output_dataset = {
        "Listed Number": num_list,
        "52Week High": high_num_list,
        "52Week Low": low_num_list,
        "Advancing Declining": None
    }
    write_xl = pd.ExcelWriter('kospi_stock_market_tech.xlsx')
    for pair in zip(output_dataset.keys(), output_dataset.values()):
        if pair[0] == "Advancing Declining":
            temporary_dataset = pd.DataFrame(list(zip(date_list, advancing_list, declining_list)))
            temporary_dataset.columns = ['Date', 'Advancing', 'Declining']
        else:
            temporary_dataset = pd.DataFrame(list(zip(date_list, pair[1])))
            temporary_dataset.columns = ['Date', 'Price']
        temporary_dataset.set_index('Date', inplace=True)
        temporary_dataset.to_excel(write_xl, sheet_name=pair[0])
    write_xl.close()


class Descriptors:
    def __init__(self, start_date, end_date, country='US'):  # date form is 'yyyy-mm-dd'
        """
        Descriptors of Fear-and-Greed Index
        :param start_date: date start
        :param end_date: date end
        :param country: US or KOR
        :param exporting_data: Exporting excel files
        """
        self.country = country
        self.date_list = pulling_dates_up(start_date, end_date, country=self.country)
        self.all_date_list = pulling_dates_up("1900-01-01", "2999-12-31", country=self.country)  # dates

    def market_momentum(self, the_period=125) -> pd.DataFrame:
        """
        a momentum of equity index
        :param the_period: moving average period
        :return: index momentum daily
        :rtype: pd.DataFrame
        """
        dataset = pd.read_excel(address, sheet_name='Main Index', usecols=[0, 1])
        dataset.columns = ['Date', 'Price']
        dataset.set_index("Date", inplace=True)
        value = []
        for i in self.date_list:
            index_price = dataset.loc[i, 'Price']
            date_index = self.all_date_list.index(i)
            the_period_date = self.all_date_list[date_index + the_period - 1]
            p = dataset.loc[i:the_period_date, 'Price']
            index_moving_average = sum(p)/the_period
            value.append(index_price - index_moving_average)
        final = pd.DataFrame(list(zip(self.date_list, value)))
        return final

    def stock_price_strength(self):
        """
        let's define the diff: # of 1 year high equities - # of 1 year low equities
        strength is the ratio, diff divided by total listed equities
        :return: strength daily
        :rtype: pd.DataFrame
        """
        high_dataset = pd.read_excel(address, sheet_name='52Week High', usecols=[0, 1])
        high_dataset.columns = ['Date', 'Price']
        high_dataset.set_index("Date", inplace=True)
        low_dataset = pd.read_excel(address, sheet_name='52Week Low', usecols=[0, 1])
        low_dataset.columns = ['Date', 'Price']
        low_dataset.set_index("Date", inplace=True)
        num_dataset = pd.read_excel(address, sheet_name='Listed Number', usecols=[0, 1])
        num_dataset.columns = ['Date', 'Price']
        num_dataset.set_index("Date", inplace=True)

        def one_row(d):
            result = {'Date': d}
            try:
                high_low_difference = high_dataset.loc[d, 'Price'] - low_dataset.loc[d, 'Price']
            except KeyError:
                high_low_difference = np.nan
            try:
                divisor = num_dataset.loc[d, 'Price']
            except KeyError:
                tem_list = list(filter(lambda x: x <= d, num_dataset.index))
                divisor = max(tem_list)
            if high_low_difference == np.nan:
                result['Ratio(%)'] = np.nan
            else:
                result['Ratio(%)'] = high_low_difference/divisor*100
            return result

        final = []
        for i in self.date_list:
            final.append(one_row(i))
        final = pd.DataFrame(final)
        final.columns = ['Date', 'Ratio(%)']
        final.set_index('Date', inplace=True)
        return final

    def market_volatility(self, the_period=50):
        """
        check volatility with VIX
        :param the_period: moving average period (business day)
        :return: volatility daily
        :rtype: pd.DataFrame
        """
        dataset = pd.read_excel(address, sheet_name="VIX", usecols=[0, 1])
        dataset.columns = ['Date', 'Price']
        dataset.set_index("Date", inplace=True)
        values = []
        for i in self.date_list:
            try:
                index_price = dataset.loc[max(list(filter(lambda x: x <= i, dataset.index))), 'Price']
                vix_index = self.all_date_list.index(i)
                the_period_date = self.all_date_list[vix_index + the_period - 1]
                vix_moving_average = np.average(dataset.loc[i:the_period_date, 'Price'])
                values.append((index_price - vix_moving_average)*(-1))
            except KeyError:
                values.append(np.nan)
        final = pd.DataFrame(list(zip(self.date_list[0:len(self.date_list) + the_period - 1], values)))
        return final

    def safe_haven_demand(self, the_period=20):
        """
        Equity's log return relative to Fixed Income
        :param the_period: return period
        :return: relative return daily
        :rtype: pd.DataFrame
        """
        bond_dataset = pd.read_excel(io=address, sheet_name="Treasury Index", usecols=[0, 1])
        stock_dataset = pd.read_excel(io=address, sheet_name="Equity Index", usecols=[0, 1])
        bond_dataset.columns = ['Date', 'Price']
        bond_dataset.set_index("Date", inplace=True)
        stock_dataset.columns = ['Date', 'Price']
        stock_dataset.set_index("Date", inplace=True)
        the_difference = []

        for i in self.date_list:
            if i in stock_dataset.index:
                pass
            else:
                i = max(list(filter(lambda x: x < i, stock_dataset.index)))
            idx = stock_dataset.index.get_loc(i)
            the_period_date = stock_dataset.index[idx + the_period]
            stock_return = np.log(stock_dataset.loc[i, 'Price']/stock_dataset.loc[the_period_date, 'Price'])*100

            if i in bond_dataset.index:
                bond_date = i
            else:
                bond_date = max(list(filter(lambda x: x < i, bond_dataset.index)))

            if the_period_date in bond_dataset.index:
                bond_the_period_date = the_period_date
            else:
                bond_the_period_date = max(list(filter(lambda x: x < the_period_date, bond_dataset.index)))
            bond_return = np.log(bond_dataset.loc[bond_date, 'Price']/bond_dataset.loc[bond_the_period_date, 'Price'])*100
            the_difference.append(stock_return - bond_return)
        final = pd.DataFrame(list(zip(self.date_list, the_difference)))
        return final

    def junk_bond_demand(self):
        """
        junk bond demand. US: High Yield - Investment Grade. KOR: (AA-) - Treasury
        :return: the minus of spread daily
        :rtype: pd.DataFrame
        """
        dataset = pd.read_excel(io=address, sheet_name="Corporate Bond Spread", usecols=[0, 1, 2])
        dataset.columns = ['Date', 'HY', 'IG']
        dataset.set_index("Date", inplace=True)
        the_spread = []
        for i in self.date_list:
            the_spread.append(dataset.loc[i, 'IG'] - dataset.loc[i, 'HY'])
        final = pd.DataFrame(list(zip(self.date_list, the_spread)))
        return final

    def put_and_call_options(self):
        """
        put option volume/call option volume
        :return: the ratio daily
        :rtype: pd.DataFrame
        """
        dataset = pd.read_excel(io=address, sheet_name="Put Call Ratio", usecols=[0, 1])
        dataset.columns = ['Date', 'Price']
        dataset.set_index("Date", inplace=True)

        the_ratio = []
        for i in self.date_list:
            if i in dataset.index:
                pass
            else:
                i = max(list(filter(lambda x: x <= i, dataset.index)))
            the_ratio.append(dataset.loc[i, 'Price']*(-1))
        final = pd.DataFrame(list(zip(self.date_list, the_ratio)))
        return final

    def stock_price_breadth(self):
        """
        McClellan Volume Summation Index.
        Advancing Equity Volume - the Declining. Exponential Moving Average
        :return: the value daily
        :rtype: pd.DataFrame
        """
        dataset = pd.read_excel(io=address, sheet_name="Advancing Declining", usecols=[0, 1, 2])
        dataset.columns = ['Date', 'Advancing', 'Declining']
        dataset.sort_values('Date', ascending=True, inplace=True)
        dataset.set_index("Date", inplace=True)
        the_difference = []
        for i in dataset.index:
            the_difference.append(dataset.loc[i, 'Advancing'] - dataset.loc[i, 'Declining'])
        dataset['Difference'] = the_difference
        ten_five = []
        ten_tem = dataset.iloc[0, 2]
        five_tem = dataset.iloc[0, 2]
        for i in dataset.index:  # Exponential Moving Average
            ten_tem = 0.9*ten_tem + 0.1*dataset.loc[i, 'Difference']
            five_tem = 0.95*five_tem + 0.05*dataset.loc[i, 'Difference']
            ten_five.append(ten_tem - five_tem)
        matrix = pd.DataFrame(list(zip(dataset.index, ten_five)))
        matrix.sort_values(0, ascending=False, inplace=True)
        return matrix[matrix[0].isin(self.date_list)]


class Scoring:
    def __init__(self, matrix):
        """
        how to calculate index from some descriptors
        :param matrix: Descriptors of Index. Date and Price. N by 2
        """
        self.matrix = matrix
        self.matrix.set_index(0, inplace=True)

    def the_number_one(self):
        value = []
        for i in self.matrix.index:
            one_year_before_date = i-relativedelta(weeks=52)
            if one_year_before_date >= self.matrix.index[-1]:  # if 1 year data exists
                one_year_before = max(list(filter(lambda x: x <= one_year_before_date, self.matrix.index)))
                one_year_average = np.mean(self.matrix.loc[i:one_year_before, 1])
                one_year_standard = np.std(self.matrix.loc[i:one_year_before, 1])
                value.append((self.matrix.loc[i, 1] - one_year_average)/one_year_standard)  # normalization
            else:
                value.append(np.nan)
        self.matrix['Value'] = value

        percentile = []
        for j in self.matrix.index:
            one_year_before_date = j - relativedelta(weeks=52)
            if j - relativedelta(weeks=52) >= self.matrix.index[len(self.matrix) - 1]:
                one_year_before = max(list(filter(lambda x: x <= one_year_before_date, self.matrix.index)))
                percentile.append((self.matrix.loc[j:one_year_before, 'Value'].rank(pct=True)[0])*100)
            else:
                percentile.append(np.nan)
        self.matrix['Percentile Rank'] = percentile
        return self.matrix


def index(start_date, end_date, country='US'):
    tem = Descriptors(start_date, end_date, country=country)
    date_list = tem.date_list
    variables = [tem.market_momentum(), tem.stock_price_strength(), tem.stock_price_breadth(), tem.put_and_call_options(),
                 tem.market_volatility(), tem.safe_haven_demand(), tem.junk_bond_demand()]
    percentile_matrix = [Scoring(x).the_number_one() for x in variables]
    columns = []
    for d in date_list:
        row = [d]
        for i in percentile_matrix:
            if d in i.index:
                row.append(i.loc[d, 'Percentile Rank'])
            else:
                row.append(np.nan)
        row.append(np.nanmean(row[1:8]))
        columns.append(row)
    final = pd.DataFrame(columns)
    two_years_later = date_list[len(date_list) - 1] + relativedelta(weeks=52*2)
    drop_index = final[final[0] == max(list(filter(lambda x: x <= two_years_later, final[0])))].index[0]
    final.drop([i for i in range(drop_index, len(date_list))], axis=0, inplace=True)
    print(final)
    final.to_excel("./abc.xlsx")
    return final


if __name__ == "__main__":
    print("pulling dates up:", pulling_dates_up("2024-01-02", "2024-01-31", country="KOR"))
    test = Descriptors("2024-01-02", "2024-01-31",  country='KOR')
    print("momentum: ", test.market_momentum())
    print("strength: ", test.stock_price_strength())
    print("volatility: ", test.market_volatility())
    print("safe heaven demand: ", test.safe_haven_demand())
    print("junk bond demand: ", test.junk_bond_demand())
    print("put call ratio: ", test.put_and_call_options())
    print("strength: ", test.stock_price_strength())
