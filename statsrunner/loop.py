import os
from lxml import etree
import inspect
import json
import sys
import traceback
import statsrunner.shared
import statsrunner.aggregate
from statsrunner.common import decimal_default


def call_stats(this_stats, args):
    """Create dictionary of enabled stats for this_stats object.

    Args:
      this_stats (cls): stats_module that specifies calculations for relevant input, processed by internal methods of process_file().

      args: Object containing program run options (set by CLI arguments at runtime. See __init__ for more details).
    """
    this_out = {}
    # For each method within this_stats object check the method is an enabled stat, if it is not enabled continue to next method.
    for name, function in inspect.getmembers(this_stats, predicate=inspect.ismethod):
        # If method is an enabled stat, add the method to the this_out dictionary, unless the exception criteria are met.
        if not statsrunner.shared.use_stat(this_stats, name):
            continue
        try:
            this_out[name] = function()
        except KeyboardInterrupt:
            exit()
        except:
            traceback.print_exc(file=sys.stdout)
    if args.debug:
        print this_out
    return this_out


def process_file((inputfile, output_dir, folder, xmlfile, args)):
    """Create output file path or write output file."""
    import importlib
    # Python module to import stats from defaults to stats.dashboard
    stats_module = importlib.import_module(args.stats_module)
    # When args.verbose_loop is true, create directory and set outputfile according to loop path.
    if args.verbose_loop:
        try:
            os.makedirs(os.path.join(output_dir, 'loop', folder))
        except OSError:
            pass
        outputfile = os.path.join(output_dir, 'loop', folder, xmlfile)
    # If args.verbose_loop is false, set outputfile according to aggregated-file path.
    else:
        outputfile = os.path.join(output_dir, 'aggregated-file', folder, xmlfile)

    # If default args is set to only create new files, check for existing file and return early.
    if args.new:
        if os.path.exists(outputfile):
            return
    # If default args are not set to only create new files try setting file_size to size of file in bytes.
    try:
        file_size = os.stat(inputfile).st_size
        # If the file size is greater than the registry limit, set stats_json file value to 'too large'.
        # Registry limit: https://github.com/okfn/ckanext-iati/blob/606e0919baf97552a14b7c608529192eb7a04b19/ckanext/iati/archiver.py#L23
        if file_size > 50000000:
            stats_json = {'file': {'toolarge': 1, 'file_size': file_size}, 'elements': []}
        # If file size is within limit, set doc to the value of the complete inputfile document, and set root to the root of element tree for doc.
        else:
            doc = etree.parse(inputfile)
            root = doc.getroot()

            def process_stats_file(FileStats):
                """Set object elements and pass to call_stats()."""
                file_stats = FileStats()
                file_stats.doc = doc
                file_stats.root = root
                file_stats.strict = args.strict
                file_stats.context = 'in '+inputfile
                file_stats.fname = os.path.basename(inputfile)
                file_stats.inputfile = inputfile
                return call_stats(file_stats, args)

            def process_stats_element(ElementStats, tagname=None):
                """Generate object elements and yield to call_stats()."""

                def is_humanitarian_activity(activity, version):
                    """Checks to see whether the activity is humanitarian.

                    This allows the Grand Bargain monitoring page to calculate stats on activities that are known to relate to humanitarian.

                    Param:
                        activity (lxml.etree._Element): A lxml representation of an <iati-activity> element.
                        version (str): The version of the IATI Standard that is used to define the activity.
                    """
                    humanitarian_sectors_dac_5_digit = ['72010', '72040', '72050', '73010', '74010']
                    humanitarian_sectors_dac_3_digit = ['720', '730', '740']

                    # Set the correct vocabulary code for the version that this activity is defined at
                    vocab_code_dac_5_digit = "DAC" if version in ["1.01", "1.02", "1.03", "1.04", "1.05"] else "1"
                    vocab_code_dac_3_digit = "DAC-3" if version in ["1.01", "1.02", "1.03", "1.04", "1.05"] else "2"

                    # use the same algorithm as _major_version() in dashboard.py
                    major_version = '2' if version and version.startswith('2.') else '1'

                    # ensure we are dealing with an activity
                    if activity.tag != 'iati-activity':
                        return False

                    # The below logic is replicated (with adapatations due to variable scope) from IATI-Stats code: ActivityStats.humanitarian()
                    # https://github.com/IATI/IATI-Stats/blob/9c3b865f6184418f854667d3bafc0be4ae835890/stats/dashboard.py#L1188-L1209

                    # logic around use of the @humanitarian attribute
                    is_humanitarian_by_attrib_activity = 1 if ('humanitarian' in activity.attrib) and (activity.attrib['humanitarian'] in ['1', 'true']) else 0
                    is_not_humanitarian_by_attrib_activity = 1 if ('humanitarian' in activity.attrib) and (activity.attrib['humanitarian'] in ['0', 'false']) else 0
                    is_humanitarian_by_attrib_transaction = 1 if set(activity.xpath('transaction/@humanitarian')).intersection(['1', 'true']) else 0
                    is_not_humanitarian_by_attrib_transaction = 1 if not is_humanitarian_by_attrib_transaction and set(activity.xpath('transaction/@humanitarian')).intersection(['0', 'false']) else 0
                    is_humanitarian_by_attrib = (version in ['2.02', '2.03']) and (is_humanitarian_by_attrib_activity or (is_humanitarian_by_attrib_transaction and not is_not_humanitarian_by_attrib_activity))

                    # logic around DAC sector codes deemed to be humanitarian
                    is_humanitarian_by_sector_5_digit_activity = 1 if set(activity.xpath('sector[@vocabulary="{0}" or not(@vocabulary)]/@code'.format(vocab_code_dac_5_digit))).intersection(humanitarian_sectors_dac_5_digit) else 0
                    is_humanitarian_by_sector_5_digit_transaction = 1 if set(activity.xpath('transaction[not(@humanitarian="0" or @humanitarian="false")]/sector[@vocabulary="{0}" or not(@vocabulary)]/@code'.format(vocab_code_dac_5_digit))).intersection(humanitarian_sectors_dac_5_digit) else 0
                    is_humanitarian_by_sector_3_digit_activity = 1 if set(activity.xpath('sector[@vocabulary="{0}"]/@code'.format(vocab_code_dac_3_digit))).intersection(humanitarian_sectors_dac_3_digit) else 0
                    is_humanitarian_by_sector_3_digit_transaction = 1 if set(activity.xpath('transaction[not(@humanitarian="0" or @humanitarian="false")]/sector[@vocabulary="{0}"]/@code'.format(vocab_code_dac_3_digit))).intersection(humanitarian_sectors_dac_3_digit) else 0

                    # helper variables to help make logic easier to read
                    is_humanitarian_by_sector_activity = is_humanitarian_by_sector_5_digit_activity or is_humanitarian_by_sector_3_digit_activity
                    is_humanitarian_by_sector_transaction = is_humanitarian_by_sector_5_digit_transaction or is_humanitarian_by_sector_3_digit_transaction
                    is_humanitarian_by_sector = is_humanitarian_by_sector_activity or (is_humanitarian_by_sector_transaction and (major_version in ['2']))

                    # combine the various ways in which an activity may be humanitarian
                    is_humanitarian = 1 if (is_humanitarian_by_attrib or is_humanitarian_by_sector) else 0
                    # deal with some edge cases that have veto
                    if is_not_humanitarian_by_attrib_activity:
                        is_humanitarian = 0

                    return bool(is_humanitarian)


                version = root.attrib.get('version', '1.01')

                hum_activities = [el for el in root if is_humanitarian_activity(el, version)]
                for element in hum_activities:
                    element_stats = ElementStats()
                    element_stats.element = element
                    element_stats.strict = args.strict
                    element_stats.context = 'in '+inputfile
                    element_stats.today = args.today
                    yield call_stats(element_stats, args)

            def process_stats(FileStats, ElementStats, tagname=None):
                """Create dictionary with processed stats_module objects.

                Args:
                    FileStats (cls): stats_module that contains calculations for an organisation or activity XML file.
                    ElementStats (cls): stats_module that contains raw stats calculations for a single organisation or activity.
                    tagname: Label for type of stats_module.

                Returns:
                    Dictionary with values that are dictionaries of the enabled stats for the file and elements being processed.
                """
                file_out = process_stats_file(FileStats)
                out = process_stats_element(ElementStats, tagname)
                return {'file': file_out, 'elements': out}

            if root.tag == 'iati-activities':
                stats_json = process_stats(stats_module.ActivityFileStats, stats_module.ActivityStats, 'iati-activity')
            elif root.tag == 'iati-organisations':
                stats_json = process_stats(stats_module.OrganisationFileStats, stats_module.OrganisationStats, 'iati-organisation')
            else:
                stats_json = {'file': {'nonstandardroots': 1}, 'elements': []}

    # If there is a ParseError print statement, then set stats_json file value according to whether the file size is zero.
    except etree.ParseError:
        print 'Could not parse file {0}'.format(inputfile)
        if os.path.getsize(inputfile) == 0:
            # Assume empty files are download errors, not invalid XML
            stats_json = {'file': {'emptyfile': 1}, 'elements': []}
        else:
            stats_json = {'file': {'invalidxml': 1}, 'elements': []}

    # If args.verbose_loop is true, assign value of list of stats_json element keys to stats_json elements key and write to json file.
    if args.verbose_loop:
        with open(outputfile, 'w') as outfp:
            stats_json['elements'] = list(stats_json['elements'])
            json.dump(stats_json, outfp, sort_keys=True, indent=2, default=decimal_default)
    # If args.verbose_loop is not true, create aggregated-file json and return the subtotal dictionary of statsrunner.aggregate.aggregate_file().
    else:
        statsrunner.aggregate.aggregate_file(stats_module, stats_json, os.path.join(output_dir, 'aggregated-file', folder, xmlfile))


def loop_folder(folder, args, data_dir, output_dir):
    """Given a folder, returns a list of XML files in folder."""
    if not os.path.isdir(os.path.join(data_dir, folder)) or folder == '.git':
        return []
    files = []
    for xmlfile in os.listdir(os.path.join(data_dir, folder)):
        try:
            files.append((os.path.join(data_dir, folder, xmlfile),
                         output_dir, folder, xmlfile, args))
        except UnicodeDecodeError:
            traceback.print_exc(file=sys.stdout)
            continue
    return files


def loop(args):
    """Loops through all specified folders to convert data to JSON output.

    Args:
        args: Object containing program run options (set by CLI arguments at runtime. See __init__ for more details).
    """
    if args.folder:
        files = loop_folder(args.folder, args, data_dir=args.data, output_dir=args.output)
    else:
        files = []
        for folder in os.listdir(args.data):
            files += loop_folder(folder, args, data_dir=args.data, output_dir=args.output)

    if args.multi > 1:
        from multiprocessing import Pool
        pool = Pool(args.multi)
        pool.map(process_file, files)
    else:
        map(process_file, files)
