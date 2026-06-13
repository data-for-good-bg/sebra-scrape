"""CLI entry point for the sebra-scrape application."""

import argparse
import logging
import os
import sys

from .gdrive import GoogleDriveUploader, UnsuccessfulFile
from .scenarios import collect_to_gdrive, collect_to_gdrive_for_days, download_for_date

logger = logging.getLogger('sebra_scrape.app')


def validate_date(date_str: str) -> str:
    """
    Validate the date format (YYYY-MM-DD).

    Args:
        date_str: Date string to validate

    Returns:
        The validated date string

    Raises:
        argparse.ArgumentTypeError: If date format is invalid
    """
    import re
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        raise argparse.ArgumentTypeError(
            f'Invalid date format: {date_str}. Expected YYYY-MM-DD'
        )
    return date_str


def cmd_list_unsuccessful(args: argparse.Namespace) -> int:
    """Handle the list-unsuccessful command."""
    creds_path = os.environ.get('SEBRA_GOOGLE_DRIVE_CREDS')
    if not creds_path:
        raise RuntimeError('SEBRA_GOOGLE_DRIVE_CREDS environment variable not set')

    folder_url = os.environ.get('SEBRA_GOOGLE_DRIVE_FOLDER_URL')
    if not folder_url:
        raise RuntimeError('SEBRA_GOOGLE_DRIVE_FOLDER_URL environment variable not set')

    uploader = GoogleDriveUploader(folder_url, creds_path)
    files = uploader.get_unsuccessful_files()

    if not files:
        logger.info('No unsuccessful files found')
        return

    for f in files:
        logger.info('File: name=%s, path=%s, id=%s, parent=%s',
                    f.name, f.relative_path, f.file_id, f.parent_folder_id)


def cmd_upload(args: argparse.Namespace) -> int:
    """Handle the upload command."""
    creds_path = os.environ.get('SEBRA_GOOGLE_DRIVE_CREDS')
    if not creds_path:
        raise RuntimeError('SEBRA_GOOGLE_DRIVE_CREDS environment variable not set')

    folder_url = os.environ.get('SEBRA_GOOGLE_DRIVE_FOLDER_URL')
    if not folder_url:
        raise RuntimeError('SEBRA_GOOGLE_DRIVE_FOLDER_URL environment variable not set')

    uploader = GoogleDriveUploader(folder_url, creds_path)
    file_id = uploader.upload_file(args.date, args.local_file)
    logger.info('Uploaded %s to Google Drive (file ID: %s)', args.local_file, file_id)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Scrape XLS files from minfin.bg transparency page'
    )

    # Global arguments
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        default=False,
        help='Enable verbose output'
    )

    # Subcommands
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Download command
    download_parser = subparsers.add_parser(
        'download',
        help='Download XLS files from minfin.bg'
    )
    download_parser.add_argument(
        '--date',
        type=validate_date,
        required=True,
        help='Date in YYYY-MM-DD format (e.g., 2026-05-20)'
    )
    download_parser.add_argument(
        '--target-dir',
        type=str,
        required=True,
        help='Target directory for downloaded files.'
    )

    # Upload command
    upload_parser = subparsers.add_parser(
        'upload',
        help='Upload file to Google Drive'
    )
    upload_parser.add_argument(
        '--date',
        type=validate_date,
        required=True,
        help='Date in YYYY-MM-DD format (e.g., 2024-05-20)'
    )
    upload_parser.add_argument(
        '--local-file',
        type=str,
        required=True,
        help='Local file path to upload'
    )

    # Collect-to-gdrive command
    collect_parser = subparsers.add_parser(
        'collect-to-gdrive',
        help='Download XLS files and upload to Google Drive'
    )
    collect_parser.add_argument(
        '--date',
        type=validate_date,
        required=True,
        help='Date in YYYY-MM-DD format (e.g., 2024-05-20)'
    )
    collect_parser.add_argument(
        '--target-dir',
        type=str,
        required=True,
        help='Target directory for local downloads'
    )
    collect_parser.add_argument(
        '--use-local-cache',
        action='store_true',
        default=False,
        help='Skip download if local date directory already exists'
    )

    # Collect-to-gdrive-for-days command
    collect_days_parser = subparsers.add_parser(
        'collect-to-gdrive-for-days',
        help='Download and upload XLS files for multiple weekdays'
    )
    collect_days_parser.add_argument(
        '--date',
        type=validate_date,
        required=True,
        help='Start date in YYYY-MM-DD format (e.g., 2024-05-20)'
    )
    collect_days_parser.add_argument(
        '--lookback-days',
        type=int,
        required=True,
        help='Number of days to look back (inclusive of start date)'
    )
    collect_days_parser.add_argument(
        '--target-dir',
        type=str,
        required=True,
        help='Target directory for local downloads'
    )
    collect_days_parser.add_argument(
        '--use-local-cache',
        action='store_true',
        default=False,
        help='Skip download if local date directory already exists'
    )

    # List-unsuccessful command
    list_unsuccessful_parser = subparsers.add_parser(
        'list-unsuccessful',
        help='List unsuccessful files in Google Drive'
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    if args.command is None:
        parser.print_help()
        return 0

    try:
        if args.command == 'download':
            download_for_date(args.date, target_dir=args.target_dir)
        elif args.command == 'upload':
            cmd_upload(args)
        elif args.command == 'collect-to-gdrive':
            collect_to_gdrive(
                date_str=args.date,
                target_dir=args.target_dir,
                use_local_cache=args.use_local_cache
            )
        elif args.command == 'collect-to-gdrive-for-days':
            collect_to_gdrive_for_days(
                date_str=args.date,
                lookback_days=args.lookback_days,
                target_dir=args.target_dir,
                use_local_cache=args.use_local_cache
            )
        elif args.command == 'list-unsuccessful':
            cmd_list_unsuccessful(args)
        else:
            raise RuntimeError('Unknown command: %s', args.command)
    except:
        logger.exception('Unexpected exception occurred.')
        return 1

if __name__ == '__main__':
    sys.exit(main())
