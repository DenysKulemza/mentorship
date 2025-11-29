# --- OOP
# --- generator vs iterator


# --- class method & encapsulation
class Animal:

    teeth = 32

    def __init__(self, name, legs, age):
        self.name = name
        self.legs = legs
        self.age = age

    def birthday(self):
        self.age += 1
        self.teeth -= 1

    def print_age(self):
        print('Name: ', self.name, ' Age: ', self.age, ' Teeth: ', self.teeth)

    @classmethod
    def start_from_scratch(cls):
        cls.teeth = 32


# --- inheritance
class Fish(Animal):

    def __init__(self, name, age, is_predator):
        super().__init__(name=name, legs=0, age=age)
        self.is_predator = is_predator

    def check_if_predator(self):
         return "Yes, it's predator" if self.is_predator else "No, it's not predator"


# --- polymorphism
class Birds(Animal):
    def __init__(self, name, age):
        super().__init__(name=name, legs=2, age=age)

    def birthday(self):
        self.teeth += 1
        self.age += 1


if __name__ == '__main__':

    bird = Birds('b', 50)

    bird.print_age()

    bird.birthday()

    bird.print_age()



    # usual_fish = Fish('u', 1, False)
    # predator_fish = Fish('p', 1, True)
    #
    # print(predator_fish.check_if_predator())

    # dog = Animal('a', 4, 1)
    # cat = Animal('b', 4, 3)
    #
    # print('Before starting from scratch')
    #
    # dog.birthday()
    # cat.birthday()
    #
    # dog.print_age()
    # cat.print_age()
    #
    # Animal.start_from_scratch()
    #
    # print('After starting from scratch')
    #
    # dog.print_age()
    # cat.print_age()


# Animal (__init__)

# self -> influence object
# cls -> influence class
