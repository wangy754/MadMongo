def main():

    more_input = True

    while more_input:

        expression = input("what would you like to know: \n"

                            "Enter 1: Find the source and destination of the bus by Vehicle ID\n"

                            "Enter 2: Find buses for a particular destination\n"

                            "Enter 3: Find route (origin and destination) by line name\n"

                            "Enter 4: Find buses that start from a given place\n"

                            "Enter 5: Given Origin name and destination name, check if there are any buses routes\n"

                            "Enter q: Exiting\n")

        if expression ==  '1':
            #Input: VehicleRef (string) Output: OriginName (string) and DestinationName (string)
            VehicleRef = input("Enter Vehicle Ref: ")
            print("Getting data for " + VehicleRef)
                  # call function 1

        elif expression == '2':
            #Input: DestinationName (string) Output: VehicleRef (string) or PublishedLineName (string)
            DestinationName = input("Enter destination: ")
            print("Getting data for " + DestinationName)          
                  # call function 2

        elif expression == '3':
            #Input: PublishedLineName (string) Output: OriginName (string) and DestinationName (string)
            PublishedLineName = input("Enter line name: ")
            print("Getting data for " + PublishedLineName)    
                 #call function 3
 
        elif expression == '4':
            #Input: OriginName (string) Output: VehicleRef (string) or PublishedLineName (string)
            OriginName = input("Enter a station name: ")
            print("Getting data for " + OriginName)    
                  # call function 4

        elif expression == '5':
            #Input: OriginName (string) and DestinationName (string) Output: PublishedLineName (string)
            StartName = input("Enter starting station: ")
            DestinationName = input("Enter destination: ")
            print("Getting data for lines from " + StartName+ " to " + DestinationName)   
                  # call function 5

        elif expression == 'q':

            more_input = False

            print("Exit and goodbye!")

        else:

            print("Please enter correct numbers !!!!\n")

if __name__ == "__main__":
    main()
