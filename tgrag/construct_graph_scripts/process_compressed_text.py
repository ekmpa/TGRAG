import glob
import os
import shutil


def move_and_rename_compressed_outputs(
    source_base: str = 'data/output_text_dir',
    target_base_root: str = 'external/cc-webgraph',
) -> None:
    """Renames and moves webgraph text output files to a unique cc-webgraph folder based on CURRENT_SLICE.

    Parameters:
        source_base (str): Base path to the source output folders.
        target_base_root (str): Root path to the target directory.
    """
    # Get current slice to make the folder unique
    slice_id = os.getenv('CURRENT_SLICE', 'default_slice')
    target_base = os.path.join(target_base_root, slice_id)
    os.makedirs(target_base, exist_ok=True)

    for subdir in ['edges', 'vertices']:
        source_dir = os.path.join(source_base, subdir)
        target_filename = f'{subdir}.txt.gz'

        # Find the .txt.gz file
        matches = glob.glob(os.path.join(source_dir, '*.txt.gz'))
        if not matches:
            raise FileNotFoundError(f'No .txt.gz file found in {source_dir}')

        source_file = matches[0]
        target_path = os.path.join(target_base, target_filename)

        shutil.move(source_file, target_path)
        print(f'Moved {source_file} → {target_path}')
