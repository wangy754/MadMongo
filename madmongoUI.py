import pymongo
from pymongo import MongoClient


def main():
    # Replace the '54.219.174.228' with the public ip of the primary machine of your AWS setup. 27017 is the default
    # mongos port. Use the port number your mongos is running on
    client = MongoClient('54.183.206.188', port=27017)
    db = client.BusdataDB
    testcoll = db.NYBusInfo

    more_input = True

    while more_input:

        expression = input("Please choose an option: \n"

                           "Enter 1: Find the source and destination of the bus by Vehicle ID\n"

                           "Enter 2: Find buses to a particular destination\n"

                           "Enter 3: Find route (origin and destination) by line name\n"

                           "Enter 4: Find buses that start from a given place\n"

                           "Enter 5: Given Origin point and Destination point, check if there are any buses plying between those places \n"

                           "Enter 6: Insert Observation\n"

                           "Enter 7: Modify Observation\n"

                           "Enter 8: Delete Observation\n"

                           "Enter 9: Find buses that begin at a particular longitude/latitude\n"

                           "Enter 10: Find buses that end at a particular longitude/latitude\n"

                           "Enter 11: Find buses that go through a particular stop  \n"

                           "Enter 12: Find the recent location of the bus with the given line name \n"

                           "Enter 13: Find the recent location of the bus with the given Vehicle ID \n"
                           
                           "Enter 14: View history of a given line name \n"

                           "Enter 15: Find the number of observations near a particular latitude \n"


                           "Enter q: Exiting\n")

        if expression == '1':
            # Input: VehicleRef (string) Output: OriginName (string) and DestinationName (string)
            # Find the bus termini of a given vehicle’s ID
            VehicleRef = input("Enter Vehicle ID: ")
            print("Getting data for " + VehicleRef)
            test_post = testcoll.find_one({'VehicleRef': VehicleRef}, {'OriginName': 1, 'DestinationName': 1, '_id': 0})
            print(
                "The source and destination of the bus with vehicle id " + VehicleRef + " is " + test_post['OriginName']
                + " and " + test_post['DestinationName'])

        elif expression == '2':
            # Input: DestinationName (string) Output: VehicleRef (string) or PublishedLineName (string)
            # Find buses that end at a particular destination or Find buses for a particular destination
            DestinationName = input("Enter destination: ")
            print("Getting data for " + DestinationName)
            test_post = testcoll.distinct("PublishedLineName", {"DestinationName": DestinationName})
            print("The bus/buses that ply to the destination " + DestinationName + " are")
            for bus in test_post:
                print(bus)
                # call function 2

        elif expression == '3':
            # Input: PublishedLineName (string) Output: OriginName (string) and DestinationName (string)
            # Find route (origin and destination) by line name
            PublishedLineName = input("Enter line name: ")
            print("Getting data for " + PublishedLineName)
            test_post = testcoll.find_one({"PublishedLineName": PublishedLineName},
                                          {'OriginName': 1, 'DestinationName': 1})
            print("This bus begins at " + test_post['OriginName'] + " and ends at " + test_post['DestinationName'])
            # call function 3

        elif expression == '4':
            # Input: OriginName (string) Output: VehicleRef (string) or PublishedLineName (string)
            OriginName = input("Enter a station name: ")
            print("Getting data for " + OriginName)
            test_post = testcoll.distinct("PublishedLineName", {"OriginName": OriginName})
            print("The bus/buses that starts at source " + OriginName + " :")
            for bus in test_post:
                print(bus)


        elif expression == '5':
            # Input: OriginName (string) and DestinationName (string) Output: PublishedLineName (string)
            StartName = input("Enter starting station: ")
            DestinationName = input("Enter destination: ")
            print("Getting data for lines from " + StartName + " to " + DestinationName)
            test_post = testcoll.distinct("PublishedLineName", {"$and": [{"OriginName": StartName},
                                                                         {"DestinationName": DestinationName}]})
            print("The bus that starts and ends at destination is ")
            for bus in test_post:
                print(bus)

        elif expression == '6':
            # Input: OriginName (string) and DestinationName (string) Output: PublishedLineName (string)
            obsv_time = input("Recorded at a time: ")
            direction_ref = input("DirectionRef: ")
            published_line_name = input("PublishedLineName: ")
            origin_name = input("OriginName: ")
            destination = input("DestinationName: ")
            vehicle_ref = input("VehicleRef ")

            print("Inserting an observation")
            insertdict = {"RecordedAtTime": obsv_time, "DirectionRef": direction_ref, "PublishedLineName": published_line_name,
                          "OriginName":origin_name,"DestinationName":destination,"VehicleRef":vehicle_ref}
            inserted = testcoll.insert_one(insertdict)
            print("Inserted Observation")
            result = testcoll.find({"PublishedLineName": published_line_name}).sort([("RecordedAtTime", -1)]).limit(1)
            print(result[0]["PublishedLineName"] + " "+result[0]["RecordedAtTime"] + " "+result[0]["OriginName"]+ " "+
                  result[0]["DestinationName"])
            # call function 6

        elif expression == '7':
            # Input: OriginName (string) and DestinationName (string) Output: PublishedLineName (string)
            column = input("Enter column name: ")
            old = input("Enter old value: ")
            new = input("Enter new value: ")
            print("Updating column " + column + " , replacing " + old + " with " + new)
            updatedict = {column: old}
            updatedict2 = {"$set": {column: new}}
            updated = testcoll.update_many(updatedict, updatedict2)
            print("Done!")
            # call function 6

        elif expression == '8':
            # Input: OriginName (string) and DestinationName (string) Output: PublishedLineName (string)
            origin = input("Enter value for OriginName: ")
            destination = input("Enter value for DestinationName: ")
            line = input("Enter value for PublishedLineName: ")
            print("Deleting documents with values " + origin + " , " + destination + " , " + destination)
            deletedict = {"OriginName": origin, "DestinationName": destination, "PublishedLineName": line}
            deleted = testcoll.delete_one(deletedict)
            print("Done!")
            # call function 6

        elif expression == '9':
            latitude = input("Enter Latitude: ")
            longitude = input("Enter Longitude: ")
            x = float(latitude)
            y = float(longitude)

            print("Fetching bus originating at " + latitude + " and " + longitude)

            test_post = testcoll.find_one({"OriginLat": x, "OriginLong": y},
                                          {'PublishedLineName': 1, 'OriginName': 1, 'DestinationName': 1})
            print("Bus/buses originating at latitude " + latitude + " and longitude " + longitude + ":")
            print("Published Line Name :" + test_post['PublishedLineName']
                  + " Origin: " + test_post['OriginName'] + " Destination: " + test_post['DestinationName'])

        elif expression == '10':
            latitude = input("Enter Latitude: ")
            longitude = input("Enter Longitude: ")
            x = float(latitude)
            y = float(longitude)

            print("Fetching Buses with destination at " + latitude + "and " + longitude)

            test_post = testcoll.find_one({"DestinationLat": x, "DestinationLong": y},
                                          {'PublishedLineName': 1, 'OriginName': 1, 'DestinationName': 1})
            print("Bus/buses ending at latitude " + latitude + " and longitude " + longitude + ":")
            print("Published Line Name :" + test_post['PublishedLineName']
                  + " Origin: " + test_post['OriginName'] + " Destination: " + test_post['DestinationName'])

        elif expression == '11':
            stop_name = input("Please enter the stop name: ")
            print("Fetching details of the buses passing through the point " + stop_name)
            result = testcoll.distinct("PublishedLineName", {"NextStopPointName": stop_name})
            print("The bus/buses passing through the given stop: ")
            for bus in result:
                print(bus)

        elif expression == '12':
            line_name = input("Enter the published line name of the bus: ")
            print("Fetching the recent location of the bus " + line_name)
            result = testcoll.find({"PublishedLineName": line_name}, {"VehicleLocation": 1}).sort(
                [("RecordedAtTime", -1)]).limit(1)
            print("The recent location of the bus "+line_name+" is: "+str(result[0]['VehicleLocation']))

        elif expression == '13':
            v_id = input("Enter the Vehicle ID: ")
            print("Fetching the recent location of the bus " + v_id)
            result = testcoll.find({"VehicleRef": v_id}, {"VehicleLocation": 1}).sort([("RecordedAtTime", -1)]).limit(1)
            print("The recent location of the bus with vehicle id: " + v_id + " is: " + str(result[0]['VehicleLocation']))
            
        elif expression ==  '14':
            LineName = input("Enter Line Name: ")
            NumEntries = int(input("Enter number of entries to view: "))
            print("Getting history for " + LineName)
            test_post = testcoll.find({'PublishedLineName':LineName},{'RecordedAtTime':1,'VehicleLocationLatitude':1,'VehicleLocationLongitude':1,'NextStopPointName':1,'ExpectedArrivalTime':1}).sort('RecordedAtTime',pymongo.DESCENDING).limit(NumEntries)
            i = 1
            for post in test_post:
                print("Entry " + str(i))
                print("Recorded at: " + post["RecordedAtTime"])
                print("Located at: (" + post["VehicleLocationLatitude"] + "," + post["VehicleLocationLongitude"] + ")")
                print("Next Stop at: " + post["NextStopPointName"])
                print("Expected Arrival at: " + str(post["ExpectedArrivalTime"]))
                i += 1

        elif expression == '15':
            latitude = input("Enter the latitude: ")
            print("Fetching data for latitude " + latitude)
            x = float(latitude)
            upper = x+ 0.001 # upper = x+ 0.01
            lower = x- 0.001 # lower = x -0.01
            
            result = db.testcoll.count_documents({"VehicleLocation.Latitude":{"$gte":lower, "$lte":upper}})
            print('number of observations near latitude ' + str(latitude) + ': ' + str(result)) 

        elif expression == 'q':

            more_input = False

            print("Exit and goodbye!")

        else:

            print("Please enter correct numbers !!!!\n")


main()
