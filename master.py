
# Data in json instead of database

customers = {
        0: "Anton",
        1: "Lucy",
        2: "Fred",
        3: "Nils",
        4: "Per",
        5: "Niklas",
        6: "Stina",
        7: "Olle",
        8: "Frida",
        9: "Björn",
        10: "Benny",
        11: "Agneta",
    }

food = {
    0: "Big Mac",
    1: "Cheeseburger",
    2: "McChicken",
    3: "Quarter Pounder",
    4: "McFeast",
}

drinks = {
            0: "Water",
            1: "Fanta",
            2: "Coke",
            3: "Milkshake",
        }


class Masterdata:
    """
        MASTERDATA

        Translating from one key to another based on object type

    """

    @staticmethod
    def translate(type: str, key: int):
        if type == "food":
            return food[key % len(food)]
        elif type == "drinks":
            return drinks[key % len(drinks)]
        elif type == "customer":
            return customers[key % len(customers)]

if __name__ == "__main__":
    for i in range(1, 15):
        print(i, Masterdata.translate(i))
