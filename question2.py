import pandas as pd
import numpy as np


# Old Capital Allocation
old_allocation = {'Account_name': ['Arbitrage','Quant','Discretionary'],
                  'Capital': [1000,500,600],
                  'allocation_fraction': [0.476190,0.238095,0.285714]
                 }
# New Capital Allocation
# Change values here
new_allocation = {'Account_name': ['Arbitrage','Quant','Discretionary', 'SEC Fines', 'lll'],
                  'allocation_fraction': [0.3,0.4,0.1,0.1, 0.1]
                 }
# Dataframe for new captial allocation
df_old = pd.DataFrame(old_allocation, columns = ['Account_name', 'Capital', 'allocation_fraction'])

df_new = pd.DataFrame(new_allocation, columns = ['Account_name', 'allocation_fraction'])

# Print old capital allocation dataframe
print(df_old)
# Print new capital allocation dataframe
print(df_new)

# Function that takes as input old capital allocation and 
# new capital allocation dataframes and rebalances portfolio accordingly
def account_transfer(df_old, df_new):
    # Total amount of capital available in the old fund
    total_capital = df_old['Capital'].sum()

    # Percentage of that capital being placed in new accounts 
    df_new['Capital'] = df_new['allocation_fraction']*total_capital
    # New capital allocation of fund for each account (this is converted to list for later use)
    new_capital_list = df_new['Capital'].tolist()
    
    # The difference between the old fund allocation and 
    # the new fund allocation for old accounts
    df_new['difference'] =  df_old['Capital'] - df_new['Capital'] 
    
    # If there is a surplus amount of money in 
    # the account mark it with a true flag.
    # These accounts need to be used to transfer money to 
    # other accounts that don't have enough money
    df_new.loc[df_new['difference'] >= 0, 'available_fund_transfer'] = True

    # If there is not enough money in 
    # the account mark it with a false flag.
    # This means other accounts with surplus money needs 
    # to transfer money to these account.
    df_new.loc[df_new['difference'] < 0, 'available_fund_transfer'] = False

    # If the value is marked as Null this means there is no money in the account
    # and money needs to be transfered from one of the other accounts.
    df_new.loc[df_new['difference'].isnull(), 'available_fund_transfer'] = False
    

    # Make a list of the accounts that don't have money from the new dataframe
    df_needs_money=df_new.loc[df_new['available_fund_transfer'] == False]
    account_needs_money=df_needs_money['Account_name'].tolist()
    print(df_new)
    
    # Make a list of the accounts that have money from the new dataframe
    df_has_money=df_new.loc[df_new['available_fund_transfer'] == True]
    account_has_money=df_has_money['Account_name'].tolist()

    # Get the money from the first accounts with and without money
    # Make that your previous money
    previous_account_has_money = account_has_money[0]
    previous_account_needs_money = account_needs_money[0]
    # Clear lists
    account_has_money.clear()
    account_needs_money.clear()
    
    # list of all the money available
    money_list = []
    # iterate through data frame and arrange values in both lists so that the money go's
    # from the accounts with surplus amounts to the accounts with less money
    for index, row in df_new.iterrows():
        # If "True" and not null the account has a surplus amount of money
        if row['available_fund_transfer'] == True and pd.notnull(row['difference']):
            # Append account to list that has money
            account_has_money.append(row['Account_name'])
            # Save the current account name that has money as the previous account name that has money
            # Do this for  later use in one of the other 2 coditions
            previous_account_has_money = row['Account_name']
            # Link it to the first account that needs money
            account_needs_money.append(previous_account_needs_money)
            # Save the ammount of surplus money in the money list
            money_list.append(row['difference'])
        # If "False" and not null the account needs money, it has a negative amount of money
        elif row['available_fund_transfer'] == False and pd.notnull(row['difference']):
            # Append account to list that needs money
            account_needs_money.append(row['Account_name'])
            # Save the current account name that has money as the previous account name that needs money
            # Do this for  later use in one of the other 2 coditions
            previous_account_needs_money = row['Account_name']
            # Link this to the previous account that has money, so money can be transfered from this account
            account_has_money.append(previous_account_has_money)
            # Appen the ammount of money needed to money list
            money_list.append(row['difference'])
        # If nothing else the value in the account is null, meaning it has zero money and needs money
        else:
            # Check account with nothing and mark it as accounts that need money
            account_needs_money.append(row['Account_name'])
            # Save the current account name that has money as the previous account name that needs money
            # Do this for  later use in one of the other 2 coditions
            previous_account_needs_money = row['Account_name']
            # Check for accounts that have money
            account_has_money.append(previous_account_has_money)
            # Money is needed (i.e. negative), check the amountof capital needed and make it negative 
            money_list.append(row['Capital']*-1)
    print(money_list)
    print(account_has_money)
    print(account_needs_money)
    print(new_capital_list)

    # Initiate money variable with zero
    money = 0
    # Previous amount of money left
    money_temp=0
    # "transfer" is variable is used for printing
    transfer= ""
    # This flag is set the moment a surplus amount of money is detected in an 
    # account after being transfered from another account
    surplus_flag = False
    for i, money_diff in enumerate(money_list):
        money = money_diff + money
        # Skip one round since we first need to compare money in first account with the next money amount
        if i >=1:
            if surplus_flag == False:
                # If there is not a surplus amount of money in the account - after transfer - print the amount
                # of money that can be send to account that needs money
                transfer = 'Send '+ str(np.abs(money_temp)) + ' from ' + account_has_money[i] + ' to '+ account_needs_money[i]
            else:
                # If there is a surplus amount of money in the account - after transfer - print the amount
                # of money that can be send from account that needed money that now has money to an account that has money    
                transfer = 'Send '+ str(np.abs(money_temp)) + ' from ' +  account_needs_money[i] + ' to '+ account_has_money[i]
                # Reset surplus flag back to "False"
                surplus_flag = False
            
            # if the capital allocation for account is larger than amount available in "money_temp" 
            money_check_value = money_temp-new_capital_list[i]
            if money_check_value >= 0:
                transfer = 'Send '+ str(money_check_value) + ' from ' + account_has_money[i]+ ' to '+ account_needs_money[i] 
                # If the value is zero the it is a "Nan" account that does not have money
                # the value is equal to the capital needed if "money_temp" is the same amount 
                if money_check_value ==0:
                    transfer = 'Send '+ str(new_capital_list[i]) + ' from ' + account_has_money[i]+ ' to '+ account_needs_money[i] 
        # If the difference for money is small (i.e. negative) and the new amount is 
        # large after account that has money was added a surplus amount remains setting the flag
        if money_diff < 0 and money > 0:
            # Set Surplus flag
            surplus_flag =True
        money_temp = money 
        print(transfer)
# Call function
account_transfer(df_old, df_new)