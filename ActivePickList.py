import os
import csv
import xml.etree.ElementTree as ET

# --------- CONFIGURATION ----------
SEARCH_PATHS = [
    r"C:\Users\samee\Desktop\SF Orgs\UXPOC Org\UXPOC\force-app\main\default\objects",
    r"C:\Users\samee\Desktop\SF Orgs\UXPOC Org\UXPOC\force-app\main\default\globalValueSets"
]
OUTPUT_FILE = "inactive_picklist_values.csv"
# ----------------------------------

def strip_namespace(tree):
    """Remove namespaces from parsed XML to simplify tag searching."""
    for elem in tree.iter():
        if '}' in elem.tag:
            elem.tag = elem.tag.split('}', 1)[1]

def parse_xml_file(file_path, object_name, picklist_name):
    """Parse an XML file and return inactive picklist values."""
    inactive_entries = []
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        strip_namespace(tree)

        picklist_type = root.findtext("type") or "UNKNOWN"

        for value in root.findall(".//value"):
            full_name = value.findtext("fullName")
            is_active = value.findtext("isActive")
            if is_active and is_active.lower() == "false":
                inactive_entries.append((
                    object_name,
                    picklist_name,
                    full_name,
                    "Inactive",
                    picklist_type
                ))

    except ET.ParseError:
        print(f"⚠️ Skipping invalid XML: {file_path}")
    except Exception as e:
        print(f"⚠️ Error processing {file_path}: {e}")
    return inactive_entries

def parse_global_value_set(file_path):
    """Parse a global value set XML file and return inactive values."""
    inactive_entries = []
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        strip_namespace(tree)
        master_label = root.findtext("masterLabel") or os.path.basename(file_path)
        for custom_value in root.findall(".//customValue"):
            full_name = custom_value.findtext("fullName")
            is_active = custom_value.findtext("isActive")
            if is_active and is_active.lower() == "false":
                inactive_entries.append((
                    "GlobalValueSet",
                    master_label,
                    full_name,
                    "Inactive",
                    "GlobalValueSet"
                ))
    except ET.ParseError:
        print(f"⚠️ Skipping invalid XML: {file_path}")
    except Exception as e:
        print(f"⚠️ Error processing {file_path}: {e}")
    return inactive_entries

def main():
    results = []

    for path in SEARCH_PATHS:
        for root_dir, _, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root_dir, file)
                if file.endswith(".field-meta.xml"):
                    # Extract Object Name and Picklist Name
                    parts = file_path.split(os.sep)
                    try:
                        obj_index = parts.index("objects") + 1
                        object_name = parts[obj_index]
                    except ValueError:
                        object_name = "UNKNOWN"

                    picklist_name = file.replace(".field-meta.xml", "")

                    results.extend(parse_xml_file(file_path, object_name, picklist_name))
                elif file.endswith(".globalValueSet-meta.xml"):
                    results.extend(parse_global_value_set(file_path))

    # Write to CSV
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Object Name", "Picklist Name", "Picklist Value", "Status", "Picklist Type"])
        writer.writerows(results)

    print(f"✅ Done! Found {len(results)} inactive picklist values.")
    print(f"📄 Output saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
