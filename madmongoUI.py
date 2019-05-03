import pymongo
from pymongo import MongoClient


def main():
    # Replace the '54.219.174.228' with the public ip of the primary machine of your AWS setup. 27017 is the default
    # mongos port. Use the port number your mongos is running on
    client = MongoClient('54.183.244.77',port=27017)
    db = client.BusdataDB
    testcoll = db.NYBusInfo
    
    more_input = True

    while more_input:

        expression = input("what would you like to know: \n"

                            "Enter 1: Find the source and destination of the bus by Vehicle ID\n"

                            "Enter 2: Find buses for a particular destination\n"

                            "Enter 3: Find route (origin and destination) by line name\n"

                            "Enter 4: Find buses that start from a given place\n"

                            "Enter 5: Given Origin name and destination name, check if there are any buses routes\n"
                            
                            "Enter 6: Insert Observation\n"
                            
                            "Enter 7: Modify Observation\n"
                            
                            "Enter 8: Delete Observation\n"
                           
                            "Enter 9: Find buses that begin at a particular longitude/latitude\n"
                           
                            "Enter 10: Find buses that go through a particular stop point \n"
                           
                            "Enter 11: Find the recent location of the bus with the given line name \n"

                            "Enter 17: Find the number of observations near a particular latitude \n"
                           

                            "Enter q: Exiting\n")

        if expression ==  '1':
            #Input: VehicleRef (string) Output: OriginName (string) and DestinationName (string)
            #Find the bus termini of a given vehicle’s ID
            VehicleRef = input("Enter Vehicle Ref: ")
            print("Getting data for " + VehicleRef)
            test_post = testcoll.find_one({'VehicleRef':VehicleRef},{'OriginName':1,'DestinationName':1})
            print(test_post)

        elif expression == '2':
            #Input: DestinationName (string) Output: VehicleRef (string) or PublishedLineName (string)
            #Find buses that end at a particular destination or Find buses for a particular destination
            DestinationName = input("Enter destination: ")
            print("Getting data for " + DestinationName)
            test_post = testcoll.distinct("PublishedLineName", {"DestinationName": DestinationName})
            print(test_post)
                  # call function 2

        elif expression == '3':
            #Input: PublishedLineName (string) Output: OriginName (string) and DestinationName (string)
            # Find route (origin and destination) by line name
            PublishedLineName = input("Enter line name: ")
            print("Getting data for " + PublishedLineName)
            test_post = testcoll.find_one({"PublishedLineName":PublishedLineName},{'OriginName':1,'DestinationName':1})
            print(test_post)
                 #call function 3
 
        elif expression == '4':
            #Input: OriginName (string) Output: VehicleRef (string) or PublishedLineName (string)
            OriginName = input("Enter a station name: ")
            print("Getting data for " + OriginName)
            test_post = testcoll.distinct("PublishedLineName", {"OriginName" : OriginName})
            print(test_post)

        elif expression == '5':
            #Input: OriginName (string) and DestinationName (string) Output: PublishedLineName (string)
            StartName = input("Enter starting station: ")
            DestinationName = input("Enter destination: ")
            print("Getting data for lines from " + StartName+ " to " + DestinationName)   
            test_post = testcoll.distinct("PublishedLineName", {"$and":[{"OriginName" : StartName},
                                                                        {"DestinationName" : DestinationName}]})
            print(test_post)
                  
        elif expression == '6':
            #Input: OriginName (string) and DestinationName (string) Output: PublishedLineName (string)
            column1 = input("Enter value for column 1: ")
            column2 = input("Enter value for column 2: ")
            column3 = input("Enter value for column 3: ")
            print("Inserting Values " + column1 + " , " + column2 + " , " + column3) 
            insertdict = { "column1": column1, "column2": column2, "column3": column3}
            inserted = testcoll.insert_one(insertdict)
            print("Done!")
                  # call function 6
                  
        elif expression == '7':
            #Input: OriginName (string) and DestinationName (string) Output: PublishedLineName (string)
            column = input("Enter column name: ")
            old = input("Enter old value: ")
            new = input("Enter new value: ")
            print("Updating column " + column + " , replacing " + old + " with " + new) 
            updatedict = {column: old}
            updatedict2 = { "$set": { column: new } }
            updated = testcoll.update_many(updatedict,updatedict2)
            print("Done!")
                  # call function 6
                  
        elif expression == '8':
            #Input: OriginName (string) and DestinationName (string) Output: PublishedLineName (string)
            column1 = input("Enter value for column 1: ")
            column2 = input("Enter value for column 2: ")
            column3 = input("Enter value for column 3: ")
            print("Deleting documents with values " + column1 + " , " + column2 + " , " + column3) 
            deletedict = { "column1": column1, "column2": column2, "column3": column3}
            deleted = testcoll.delete_one(deletedict)
            print("Done!")
                  # call function 6

        elif expression == '9':
            latitude = input("Enter Latitude: ")
            longitude = input("Enter Longitude: ")
            x = float(latitude)
            y= float(longitude)

            print("Fetching Buses originating at "+latitude+"and "+longitude)

            test_post = testcoll.find_one({"OriginLat": x, "OriginLong": y },{'PublishedLineName':1, 'OriginName':1,'DestinationName':1})
            print(test_post)
            
        elif expression == '10':
            stop_name = input("Please enter the stop name: ")
            print("Fetching details of the buses passing through the point "+ stop_name)
            result = testcoll.distinct("PublishedLineName",{"NextStopPointName" : stop_name})
            print(result)

        elif expression == '11':
            line_name = input("Enter the published line name of the bus: ")
            print("Fetching the recent location of the bus "+line_name)
            result = testcoll.find({"PublishedLineName":line_name},{"VehicleLocation":1}).sort([("RecordedAtTime",-1)]).limit(1)
            print(result[0])


        elif expression == '17':
            line_name = input("Enter the published line name of the bus: ")
            print("Fetching data for latitude "+line_name)
            result = testcoll.find({"PublishedLineName":line_name}).sort([("RecordedAtTime",-1)])
            for doc in result:
                print(doc) 

        elif expression == 'q':

            more_input = False

            print("Exit and goodbye!")

        else:

            print("Please enter correct numbers !!!!\n")

main()
