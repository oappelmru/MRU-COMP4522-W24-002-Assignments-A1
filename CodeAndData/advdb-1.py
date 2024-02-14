# Adv DB Winter 2024 - 1

# test save 
# import random
# import csv 

# data_base = []  # Global binding for the Database contents

# '''
# transactions = [['id1',' attribute2', 'value1'], ['id2',' attribute2', 'value2'],
#                 ['id3', 'attribute3', 'value3']]
# '''
# transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
#                 ['15', 'Salary', '200000']]
# DB_Log = [] # <-- You WILL populate this as you go


# # Status constants

# def recovery_script(log:list):  #<--- Your CODE
#     '''
#     Restore the database to stable and sound condition, by processing the DB log.
#     '''
#     print("Calling your recovery script with DB_Log as an argument.")
#     print("Recovery in process ...\n")

#     global data_base

   
#     for transaction in log:
#         transaction_id, attribute, old_value, new_value = transaction

#         # Undo (Rollback)
#         for record in data_base:
#             if record[0] == transaction_id and record[1] == attribute:
#                 record[2] = old_value
#                 break
#         #do it again
#         for record in data_base:
#             if record[0] == transaction_id and record[1] == attribute:
#                 record[2] = new_value
#                 break

   
#     print("Recovery successful! Changes committed.")

#     log.clear()
   
#     pass

# def transaction_processing(): #<-- Your CODE
#     '''
#     1. Process transaction in the transaction queue.
#     2. Updates DB_Log accordingly
#     3. This function does NOT commit the updates, just execute them
#     '''

#     global data_base
#     global transactions
#     global DB_Log

#     for transaction in transactions:
#         transaction_id, attribute, new_value = transaction

       
#         record = next((record for record in data_base if record[0] == transaction_id and record[1] == attribute), None)

#         if record:
            
#             old_value = record[2]
#             DB_Log.append([transaction_id, attribute, old_value, new_value])

           
#             record[2] = new_value
#             print(f"Transaction processed: {transaction}")
#         else:
#             print(f"Record with ID {transaction_id} and attribute {attribute} not found.")

#     pass
    

# def read_file(file_name:str)->list:
#     '''
#     Read the contents of a CSV file line-by-line and return a new CSV file
#     '''
#     data = []
 
#     with open(file_name, 'r') as csv_file:
#         csv_reader = csv.reader(csv_file)

#         with open('NEW_EMP.csv','w') as new_file:
#             csv_writer = csv.writer(new_file, delimiter=',')

#             for line in csv_reader:
#                 csv_writer.writerow(line)

#         # line = reader.readline()
#         # while line != '':  
#         #     line = line.strip().split(',')
#         #     data.append(line)
          
#         #     line = reader.readline()

#     size = len(data)
#     print('The data entries BEFORE updates are presented below:')
#     for item in data:
#         print(item)
#     print(f"\nThere are {size} records in the database, including one header.\n")
#     return data

# def is_there_a_failure()->bool:
#     '''
#     Simulates randomly a failure, returning True or False, accordingly
#     '''
#     value = random.randint(0,1)
#     if value == 1:
#         result = True
#     else:
#         result = False
#     return result

# def main():
#     # number_of_transactions = len(transactions)
#     # must_recover = False
#     # data_base = read_file('./CodeAndData/Employees_DB_ADV.csv')
#     # failure = is_there_a_failure()
#     # failing_transaction_index = None
#     # while not failure:
#     #     # Process transaction
#     #     for index in range(number_of_transactions):
#     #         print(f"\nProcessing transaction No. {index+1}.")    #<--- Your CODE (Call function transaction_processing)
#     #         transaction_processing()
#     #         print("UPDATES have not been committed yet...\n")
#     #         failure = is_there_a_failure()
#     #         if failure:
#     #             must_recover = True
#     #             failing_transaction_index = index + 1
#     #             print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
#     #             break
#     #         else:
#     #             print(f'Transaction No. {index+1} has been commited! Changes are permanent.')
                
#     # if must_recover:
#     #     #Call your recovery script
#     #     recovery_script(DB_Log) ### Call the recovery function to restore DB to sound state
#     # else:
#     #     # All transactiones ended up well
#     #     print("All transaction ended up well.")
#     #     print("Updates to the database were committed!\n")

#     # print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
#     # for item in data_base:
#     #     print(item)

#      number_of_transactions = len(transactions)
#      must_recover = False
#      data_base = read_file('./CodeAndData/Employees_DB_ADV.csv')
#      failure = is_there_a_failure()
#      failing_transaction_index = None
#      while not failure:
#         # Process transaction
#         for index in range(number_of_transactions):
#             print(f"\nProcessing transaction No. {index + 1}.")
#             transaction_processing()
#             print("UPDATES have not been committed yet...\n")
#             failure = is_there_a_failure()
#             if failure:
#                 must_recover = True
#                 failing_transaction_index = index + 1
#                 print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
#                 break
#             else:
#                 print(f'Transaction No. {index + 1} has been commited! Changes are permanent.')

#      if must_recover:
#         # Call your recovery script
#         recovery_script(DB_Log)
#      else:
#         # All transactions ended up well
#         print("All transaction ended up well.")
#         print("Updates to the database were committed!\n")

#     # Write NEW_EMP.csv with updated states
#      with open('NEW_EMP.csv', 'w', newline='') as new_file:
#         csv_writer = csv.writer(new_file)
#         csv_writer.writerow(['Unique_ID', 'First_name', 'Last_name', 'Salary', 'Department', 'Civil_status', 'State'])  # Add 'State' here
#         csv_writer.writerows(data_base)

#      print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
#      for item in data_base:
#         print(item)

    
# main()

# Adv DB Winter 2024 - 1
# test save 
# Adv DB Winter 2024 - 1
# test save 

# Adv DB Winter 2024 - 1
# test save 

# Adv DB Winter 2024 - 1
# test save 

# Adv DB Winter 2024 - 1
import random
import csv

data_base = []  # Global binding for the Database contents

transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
                ['15', 'Salary', '200000']]
DB_Log = []  # You WILL populate this as you go

# Status constants
NOT_COMMITTED = "not-committed"
ROLLED_BACK = "rolled back"
COMMITTED = "committed"

def recovery_script(log: list):
    '''
    Restore the database to a stable and sound condition by processing the DB log.
    '''
    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")

    global data_base

    for i in range(len(log)):
        entry = log[i]
        transaction_id, attribute, old_value, state = entry

        if state == 'not committed':
            record = next((record for record in data_base if record[0] == transaction_id and record[1] == attribute), None)
            update_database(data_base, transaction_id, attribute, old_value)
            log[i] = (transaction_id,attribute, old_value, 'rolled-back')
            if record:
                update_database(data_base, transaction_id, attribute, old_value)
                log[i] = (transaction_id,attribute, old_value, 'rolled-back')
    new_file_name = 'Logging.csv'
    with open(new_file_name, 'w', newline='') as writer:
        csv_writer = csv.writer(writer)
        csv_writer.writerow(['Unique_ID', 'Category', 'New_Value', 'Status'])
        csv_writer.writerows(log)
                    
    print("Recovery successful! Changes committed.")


    log.clear()

def transaction_processing(transactions):
    '''
    1. Process transaction in the transaction queue.
    2. Updates DB_Log accordingly
    3. This function does NOT commit the updates, just execute them
    '''
    global data_base
    
    global DB_Log

    for transaction in transactions:
        unique_id, category, value = transaction

        if unique_id == '1' and category == 'Department':
            # Update the value in DB_Log
            DB_Log.append((unique_id, category, value, 'not committed'))
        elif unique_id == '5' and category == 'Civil_status':
            # Update the value in DB_Log
            DB_Log.append((unique_id, category, value, 'not committed'))
        elif unique_id == '15' and category == 'Salary':
            # Update the value in DB_Log
            DB_Log.append((unique_id, category, value, 'not committed'))
        else:
            # Log the transaction with a default status of 'not committed'
            DB_Log.append((unique_id, category, value, 'not committed'))

    # Print the updated DB_Log for demonstration purposes
    print("Updated DB_Log:", DB_Log)


def read_file(file_name: str) -> list:
    '''
    Read the contents of a CSV file line-by-line and return a new CSV file
    '''
    data = []

    with open(file_name, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        for line in csv_reader:
            data.append(line + [NOT_COMMITTED])  # Initialize state as "not-committed"
            print(line)

    size = len(data)
    print('The data entries BEFORE updates are presented below:')
    for item in data:
        print(item)
    print(f"\nThere are {size} records in the database, including one header.\n")
    return data

def is_there_a_failure() -> bool:
    '''
    Simulates randomly a failure, returning True or False, accordingly
    '''
    value = random.randint(0, 1)
    if value == 1:
        result = True
    else:
        result = False
    return result

def update_database(database, unique_id, category, new_value):
    '''Update the data_base list with the changes'''
    for entry in database:
        if entry[0] == unique_id and entry[1] == category:
            entry[2] = new_value
            break

def main():
    global data_base
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = read_file('./CodeAndData/Employees_DB_ADV.csv')
    failure = is_there_a_failure()
    failing_transaction_index = None

    while not failure:
        # Process transaction
        for index in range(number_of_transactions):
            print(f"\nProcessing transaction No. {index + 1}.")
            transaction_processing([transactions[index]])
            print("UPDATES have not been committed yet...\n")
            failure = is_there_a_failure()
            if failure:
                must_recover = True
                failing_transaction_index = index + 1
                print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
                break
            else:
                print(f'Transaction No. {index + 1} has been committed! Changes are permanent.')
                for i in range(len(DB_Log)):
                    entry = DB_Log[i]
                    unique_id, category, new_value, status = entry
                    if failure == False and status == 'not committed':
                        update_database(data_base, unique_id, category, new_value)
                        DB_Log[i] = (unique_id, category, new_value, 'committed')
    
    if must_recover:
        recovery_script(DB_Log)

        print("Rollback completed successfully.\n") ### Call the recovery function to restore DB to sound state

    else:
        # All transactions ended up well
        
        print("All transaction ended up well.")
        print("Updates to the database were committed!\n")

    # Write NEW_EMP.csv with updated states
    with open('NEW_EMP.csv', 'w', newline='') as new_file:
        csv_writer = csv.writer(new_file)
        csv_writer.writerow(['Unique_ID', 'First_name', 'Last_name', 'Salary', 'Department', 'Civil_status', 'State'])
        csv_writer.writerows(data_base)

   
        

    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
    for item in data_base:
        print(item)

  

main()

