from oxidizer_lite.residue import Residue


class Incubation(Residue):
    """
    The Incubation Schedules Cron Jobs - Lattice, Run, Node
    """
    def __init__(self):
        """
        Initializes the Incubation scheduler for managing cron jobs.
        """
        super().__init__(component_name="incubation")
        self.jobs = None
        self.running = None 


    def add_schedule(self, lattice_id, schedule, function, run_id=None, node_id=None):
        """
        Adds a scheduled job to the incubation.
        
        Args:
            lattice_id (str): The ID of the lattice for which the schedule is being added.
            schedule (str): The cron schedule string (e.g., "0 * * * *" for hourly).
            function (callable): The function to execute when the schedule triggers.
            run_id (str | None): The ID of the run for which the schedule is being added.
            node_id (str | None): The ID of the node for which the schedule is being added.
        """
        # Implementation to add the scheduled job to the incubation scheduler
        pass

    def list_schedules(self):
        """
        Lists all scheduled jobs in the incubation.
        
        Returns:
            list: A list of scheduled jobs with their details.
        """
        # Implementation to list all scheduled jobs in the incubation scheduler
        pass

    def remove_schedule(self, name):
        """
        Removes a scheduled job from the incubation.
        
        Args:
            name (str): The name of the scheduled job to remove.
        """
        # Implementation to remove the scheduled job from the incubation scheduler
        pass

    def start(self):
        """
        Starts the incubation scheduler to begin executing scheduled jobs.
        """
        # Implementation to start the incubation scheduler
        pass

    def stop(self):
        """
        Stops the incubation scheduler from executing scheduled jobs.
        """
        # Implementation to stop the incubation scheduler
        pass

    