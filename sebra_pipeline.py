class SebraParser:
    def __init__(self, data_url, file_location) -> None:
        self.url = data_url
        self.file_location = file_location
    
    def download_report(self):
        """
        Downloads the SEBRA reports from the url.

        [extended_summary]
        """
        pass

    def read_files(self):
        """
        Reads downloaded report files from local directory

        [extended_summary]
        """
        pass

    def parse_files(self):
        """
        Parses the individual files into an Excel

        [extended_summary]
        """
        pass

    def upload_to_gc(self, remove_from_local = True):
        """
        Uploads raw reports and the combined data into relevant GCloud folder

        [extended_summary]

        Args:
            remove_from_local (bool, optional): Flag to remove all files from the local directory. Defaults to True.
        """
        pass