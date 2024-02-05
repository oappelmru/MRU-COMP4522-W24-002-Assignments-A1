# Adv DB Winter 2024 - 1

# test save 
import random
import csv 

data_base = []  # Global binding for the Database contents

'''
transactions = [['id1',' attribute2', 'value1'], ['id2',' attribute2', 'value2'],
                ['id3', 'attribute3', 'value3']]
'''
transactions = [['1', 'Department', 'Music'], ['5', 'Civil_status', 'Divorced'],
                ['15', 'Salary', '200000']]
DB_Log = [] # <-- You WILL populate this as you go


# Status constants

def recovery_script(log:list):  #<--- Your CODE
    '''
    Restore the database to stable and sound condition, by processing the DB log.
    '''
    print("Calling your recovery script with DB_Log as an argument.")
    print("Recovery in process ...\n")

    global data_base

   
    for transaction in log:
        transaction_id, attribute, old_value, new_value = transaction

        # Undo (Rollback)
        for record in data_base:
            if record[0] == transaction_id and record[1] == attribute:
                record[2] = old_value
                break
        #do it again
        for record in data_base:
            if record[0] == transaction_id and record[1] == attribute:
                record[2] = new_value
                break

   
    print("Recovery successful! Changes committed.")

    log.clear()
   
    pass

def transaction_processing(): #<-- Your CODE
    '''
    1. Process transaction in the transaction queue.
    2. Updates DB_Log accordingly
    3. This function does NOT commit the updates, just execute them
    '''

    global data_base
    global transactions
    global DB_Log

    for transaction in transactions:
        transaction_id, attribute, new_value = transaction

       
        record = next((record for record in data_base if record[0] == transaction_id and record[1] == attribute), None)

        if record:
            
            old_value = record[2]
            DB_Log.append([transaction_id, attribute, old_value, new_value])

           
            record[2] = new_value
            print(f"Transaction processed: {transaction}")
        else:
            print(f"Record with ID {transaction_id} and attribute {attribute} not found.")

    pass
    

def read_file(file_name:str)->list:
    '''
    Read the contents of a CSV file line-by-line and return a new CSV file
    '''
    data = []
 
    with open(file_name, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        with open('NEW_EMP.csv','w') as new_file:
            csv_writer = csv.writer(new_file, delimiter=',')

            for line in csv_reader:
                csv_writer.writerow(line)

        # line = reader.readline()
        # while line != '':  
        #     line = line.strip().split(',')
        #     data.append(line)
          
        #     line = reader.readline()

    size = len(data)
    print('The data entries BEFORE updates are presented below:')
    for item in data:
        print(item)
    print(f"\nThere are {size} records in the database, including one header.\n")
    return data

def is_there_a_failure()->bool:
    '''
    Simulates randomly a failure, returning True or False, accordingly
    '''
    value = random.randint(0,1)
    if value == 1:
        result = True
    else:
        result = False
    return result

def main():
    number_of_transactions = len(transactions)
    must_recover = False
    data_base = read_file('./CodeAndData/Employees_DB_ADV.csv')
    failure = is_there_a_failure()
    failing_transaction_index = None
    while not failure:
        # Process transaction
        for index in range(number_of_transactions):
            print(f"\nProcessing transaction No. {index+1}.")    #<--- Your CODE (Call function transaction_processing)
            transaction_processing()
            print("UPDATES have not been committed yet...\n")
            failure = is_there_a_failure()
            if failure:
                must_recover = True
                failing_transaction_index = index + 1
                print(f'There was a failure whilst processing transaction No. {failing_transaction_index}.')
                break
            else:
                print(f'Transaction No. {index+1} has been commited! Changes are permanent.')
                
    if must_recover:
        #Call your recovery script
        recovery_script(DB_Log) ### Call the recovery function to restore DB to sound state
    else:
        # All transactiones ended up well
        print("All transaction ended up well.")
        print("Updates to the database were committed!\n")

    print('The data entries AFTER updates -and RECOVERY, if necessary- are presented below:')
    for item in data_base:
        print(item)

    
main()



