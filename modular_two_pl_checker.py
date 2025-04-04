from copy import deepcopy
from schedules import *
import time


# Class that checks whether a schedule follows the Two-Phase Locking (2PL) protocol
class Modular_TwoPLChecker:

    # Constructor to initialize the required parameters
    def __init__(self, n_transactions, schedule, resources, resources_needed={}, resources_to_use={}):
        self.n_transactions = n_transactions
        self.schedule = schedule  # List of operations in the schedule
        self.resources = resources  # Set of all resources involved
        self.resources_needed = resources_needed  # Resources each transaction will need
        self.resources_to_use = resources_to_use  # Resources each transaction will actually use

    # Function to check if other transactions can anticipate their locks to allow this one to proceed
    def anticipate_locks(self, action, asking_transaction, resource, loop = []):
                   
        loop.append((action, asking_transaction, resource))
        # Second loop: Check if those transactions can anticipate all their other locks
        for other_transaction in self.lock[str(resource)][1]:
            if other_transaction == asking_transaction:
                continue
            # First loop: Check if the other transactions that hold the lock still need to use the resource  
            # Another transaction still needs the resource → can't release it
            for for_resource, _  in self.resources_to_use[str(other_transaction)]:
                if for_resource == resource:
                    return False
            r_n = deepcopy(self.resources_needed[str(other_transaction)])
            # Second loop: Check if the other transactions that hold the lock can anticipate
            for (other_resource, other_action) in r_n:
                other_tuple = (other_action, other_transaction, other_resource)
                if other_tuple in loop:
                    return False  # Avoid infinite loops / circular dependencies
                l = deepcopy(loop)
                result = self.check_if_action_legal(other_transaction, other_action, other_resource, True, l)
                if not result:
                    return False

        return True  # All checks passed → it's safe to anticipate

    
    def acquire_new_lock(self, transaction, action, resource, anticipating):
        self.lock[resource] = (action, [transaction])  # Assign the lock
        self.resources_needed[str(transaction)].remove((resource, action))  # Mark resource as locked
        if not self.resources_needed[str(transaction)]:
            self.phase[str(transaction)] = False
        if not anticipating:
            self.resources_to_use[str(transaction)].remove((resource, action))
            if not self.phase[str(transaction)]:
                self.lock[resource] = None  # Release lock

        return True

    
    def has_lock(self, transaction, action, resource):
        lock_type, holders = self.lock[resource]
        return (lock_type == "W" and transaction in holders) or \
               (action == "R" and lock_type == "R" and transaction in holders)

    
    def use_acquired_lock(self, transaction, action, resource):
        
        # Remove the resource (already) locked by the list
        self.resources_to_use[str(transaction)].remove((resource, action))
        # If i don't have any other resource to lock
        if not self.resources_to_use[str(transaction)]:
            self.phase[str(transaction)] = False  # Enter shrinking phase, maybe use function to ensure lo leave locks that has been taken during growing phase, used and never left? If not function, ok beacuse it work but implicitly
            if action == "W":
                self.lock[resource] = None
            else:
                # Otherwise (i.e., if it was a read), remove the transaction from the shared read lock.
                self.lock[resource][1].remove(transaction)
                # If no other transactions are holding the read lock, then release it entirely.
                if not self.lock[resource][1]:
                    self.lock[resource] = None
        return True

    
    def handle_read_lock(self, action, transaction, resource, anticipating, loop):
        if not self.phase[str(transaction)]:
            return False  # Can't acquire locks in shrinking phase

        lock_type, holders = self.lock[resource]

        if lock_type == "R":
            # Grant shared read lock
            holders.append(transaction)
            self.resources_needed[str(transaction)].remove((resource, "R"))
            if not self.resources_needed[str(transaction)]:
                self.phase[str(transaction)] = False
            if not anticipating:
                self.resources_to_use[str(transaction)].remove((resource, "R"))
                if not self.phase[str(transaction)]:
                    holders.remove(transaction)
                    if not holders:
                        self.lock[resource] = None
            return True
        #No shared lock possible, can holder release it so I can read?
        elif self.anticipate_locks(action, transaction, resource, loop):
            return self.acquire_new_lock(transaction, "R", resource, anticipating)

        return False
    
    def handle_write_lock(self, action, transaction, resource, anticipating, loop):
        if not self.phase[str(transaction)]:
            return False

        #Either upgrade lock or acquiring from scratch
        if self.lock[resource] == ("R", [transaction]) or self.anticipate_locks(action, transaction, resource, loop):
            self.lock[resource] = ("W", [transaction])
            self.resources_needed[str(transaction)].remove((resource, "W"))
            if not self.resources_needed[str(transaction)]:
                self.phase[str(transaction)] = False
            if not anticipating:
                self.resources_to_use[str(transaction)].remove((resource, "W"))
                if not self.phase[str(transaction)]:
                    self.lock[resource] = None
            return True

        return False

    
    def check_if_action_legal(self, transaction, action, resource, anticipating=False, loop=[]):

        # Case 1: Resource is free -> none has locked the resource and the transaction is in growing phase
        if self.lock[resource] is None and self.phase[str(transaction)]:
            return self.acquire_new_lock(transaction, action, resource, anticipating)

        # Case 2: Transaction already holds the needed lock -> 
        elif not anticipating and self.has_lock(transaction, action, resource):
            return self.use_acquired_lock(transaction, action, resource)

        # Case 3: Resource is locked, but a shared read is still possible
        elif action == "R":

            return self.handle_read_lock(action, transaction, resource, anticipating, loop)

        # Case 4: Handle write request (either from scratch or upgrade)
        elif action == "W":
            return self.handle_write_lock(action, transaction, resource, anticipating, loop)
        
        else:
            return False

    # Prepare the data structures needed for the 2PL check
    def parse(self):
        for operation in self.schedule:
            action, tr, resource = operation

            if str(tr) not in self.resources_needed.keys():
                self.resources_needed[str(tr)] = [(resource, action)]
                self.resources_to_use[str(tr)] = [(resource, action)]
            else:
                self.resources_needed[str(tr)].append((resource, action))
                self.resources_to_use[str(tr)].append((resource, action))

    # Initialize locks and transaction phases
    def parse_lock(self):    
        self.lock = {}  # Dict mapping each resource to its current lock
        self.phase = {}  # Indicates if a transaction is still in the growing phase
        
        for resource in self.resources:
            self.lock[resource] = None  # Initially, no resource is locked
            
        for i in range(0, self.n_transactions):
            self.phase[str(i)] = True  # All transactions start in growing phase

    # Main function to check if the entire schedule respects 2PL
    def two_pl_checker(self):
        for operation in self.schedule:
            #print(operation)
            #print(self.lock)
            action, tr, resource = operation

            result = self.check_if_action_legal(tr, action, resource, False, [])
            #print(self.lock)
            #print("--------------------")
            if not result:
                return False  # If even one operation violates 2PL → invalid schedule

        return True  # All operations respected 2PL
'''
# Example main logic (incorrect __name__ check fixed below)
if __name__ == "__main__":
    for s in schedules:
        info = s.pop(0)  # Remove metadata or description line
        schedule = s
        n_transactions = info[0]
        resources = info[1]
        
        checker = Modular_TwoPLChecker(n_transactions, schedule, resources)
        checker.parse()
        checker.parse_lock()
        result = checker.two_pl_checker()

        
        print(f'Schedule: {schedule} is 2PL? {result}')'
'''
if __name__ == "__main__":
    start_time = time.perf_counter() # Avvia il timer
    for i in range(0,100000):
        schedules_copy = deepcopy(schedules)  # copia completa dei dati
        for schedule in schedules_copy:
            info = schedule.pop(0)
            n_transactions = info[0]
            resources = info[1]
            pl = Modular_TwoPLChecker(n_transactions, schedule, resources, {}, {})
            pl.parse()
            pl.parse_lock()
            result = pl.two_pl_checker()
            #print(f'Lo schedule: {schedule} è 2pl? {pl.two_pl_checker()}')

    end_time = time.perf_counter()  # Ferma il timer 
    delta = (end_time-start_time)
    print(f"TWO_PL_LOCKFUL - Elapsed time: {delta}\n*****************")
