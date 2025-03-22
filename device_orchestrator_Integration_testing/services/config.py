import asyncio
import uuid
from datetime import datetime, timezone

command_id = str(uuid.uuid4())
trace_id = str(uuid.uuid4())
correlation_id=str(uuid.uuid4())

# Generate current timestamp in ISO 8601 format with UTC
timestamp = (datetime.now(timezone.utc).isoformat(timespec='microseconds')
             .replace('+00:00', 'Z'))

def generate_orchestrator_command_start_pbs_job(correlation_id,trace_id):
    """Generate a ORCHESTRATOR_COMMAND_START_PBS_JOB with a  UUID"""
    return {
        "id": str(uuid.uuid4()),
        "type": "start_pbs_job",
        "version": 1,
        "data": {
            "tray_type": "all_pbs",
            "scanned_by": "",
            "workflow_mode": "manual",
            "timezone": ""
        },
        "metadata": {
            "correlation_id": None,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "trace_id": trace_id
        }
    }

def generate_orchestrator_command_next_on_place_tray_screen(correlation_id,trace_id):
      return {
          "id": str(uuid.uuid4()),
          "type": "next_on_place_tray_screen", "version": 1,
          "data": {},
           "metadata": {
              "correlation_id": correlation_id,
               "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "trace_id": trace_id
              }
      }

def generate_orchestrator_command_start_pre_processing_slide_data(correlation_id,trace_id):
    return {
    "id":command_id,
    "type": "start_pre_processing_slide_data",
    "version": 1,
    "data": {
      "slide_data": [
        {
          "slot_id": "1",
          "extracted_barcode": "SIG0058",
          "slot_image": {
            "path": "/imgarc/nila/data/shared/25b29887-0ef5-4511-a68e-9ea665939d5e/slot_1/cropped_fused_04dcfd14-b92b-11ef-8b83-0242ac120019.jpg",
            "ppm": "0.0451"
          },
          "is_empty": "False",
          "macro_image": "",
          "reviewer": {
            "id": "",
            "name": ""
          },
          "description": "",
          "scan_status": "pending",
          "slide_id": "SIG0058",
          "mode": "",
          "valid": True,
          "err_msg": ""
        },
        {
          "slot_id": "2",
          "extracted_barcode": "no_barcode_detected",
          "slot_image": {
            "path": "/imgarc/nila/data/shared/25b29887-0ef5-4511-a68e-9ea665939d5e/slot_2/cropped_fused_0843b13c-b92b-11ef-8b83-0242ac120019.jpg",
            "ppm": "0.0451"
          },
          "is_empty": "True",
          "macro_image": "",
          "reviewer": {
            "id": "",
            "name": ""
          },
          "description": "",
          "scan_status": "pending",
          "slide_id": "",
          "mode": "",
          "valid": False,
          "err_msg": "L'ID de la diapositive ne doit pas \u00eatre vide ou commencer par PB, BFS, ERR, QC ou espace"
        },
        {
          "slot_id": "3",
          "extracted_barcode": "no_barcode_detected",
          "slot_image": {
            "path": "/imgarc/nila/data/shared/25b29887-0ef5-4511-a68e-9ea665939d5e/slot_3/cropped_fused_0dae64fa-b92b-11ef-8b83-0242ac120019.jpg",
            "ppm": "0.0451"
          },
          "is_empty": "True",
          "macro_image": "",
          "reviewer": {
            "id": "",
            "name": ""
          },
          "description": "",
          "scan_status": "pending",
          "slide_id": "",
          "mode": "",
          "valid": False,
          "err_msg": "L'ID de la diapositive ne doit pas \u00eatre vide ou commencer par PB, BFS, ERR, QC ou espace"
        },
        {
          "slot_id": "4",
          "extracted_barcode": "no_barcode_detected",
          "slot_image": {
            "path": "/imgarc/nila/data/shared/25b29887-0ef5-4511-a68e-9ea665939d5e/slot_4/cropped_fused_131c2eb8-b92b-11ef-8b83-0242ac120019.jpg",
            "ppm": "0.0451"
          },
          "is_empty": "True",
          "macro_image": "",
          "reviewer": {
            "id": "",
            "name": ""
          },
          "description": "",
          "scan_status": "pending",
          "slide_id": "",
          "mode": "",
          "valid": False,
          "err_msg": "L'ID de la diapositive ne doit pas \u00eatre vide ou commencer par PB, BFS, ERR, QC ou espace"
        },
        {
          "slot_id": "5",
          "extracted_barcode": "no_barcode_detected",
          "slot_image": {
            "path": "/imgarc/nila/data/shared/25b29887-0ef5-4511-a68e-9ea665939d5e/slot_5/cropped_fused_186d6224-b92b-11ef-8b83-0242ac120019.jpg",
            "ppm": "0.0451"
          },
          "is_empty": "true",
          "macro_image": "",
          "reviewer": {
            "id": "",
            "name": ""
          },
          "description": "",
          "scan_status": "pending",
          "slide_id": "",
          "mode": "",
          "valid": False,
          "err_msg": "L'ID de la diapositive ne doit pas \u00eatre vide ou commencer par PB, BFS, ERR, QC ou espace"
        },
        {
          "slot_id": "6",
          "extracted_barcode": "no_barcode_detected",
          "slot_image": {
            "path": "/imgarc/nila/data/shared/25b29887-0ef5-4511-a68e-9ea665939d5e/slot_6/cropped_fused_1dc43644-b92b-11ef-8b83-0242ac120019.jpg",
            "ppm": "0.0451"
          },
          "is_empty": "true",
          "macro_image": "",
          "reviewer": {
            "id": "",
            "name": ""
          },
          "description": "",
          "scan_status": "pending",
          "slide_id": "",
          "mode": "",
          "valid": False,
          "err_msg": "L'ID de la diapositive ne doit pas \u00eatre vide ou commencer par PB, BFS, ERR, QC ou espace"
        }
      ]
    },
    "metadata": {
      "correlation_id": correlation_id,
      "timestamp": timestamp,
      "trace_id": trace_id
    }
  }

def generate_orchestrator_command_show_macro_scan_progress_screen(correlation_id,trace_id):
    return {
    "id": str(uuid.uuid4()),
     "type": "show_macro_scan_progress_screen",
     "version": 1,
     "data": {},
     "metadata":
         {
            "correlation_id":None,
            "timestamp": "2024-12-03 12:35:22",
            "trace_id": trace_id
          }
    }

def generate_orchestrator_command_start_macro_scan(correlation_id,trace_id):
    return {
        "id": str(uuid.uuid4()),
            "type": "start_macro_scan", "version": 1,
            "data": {"macro_scan_session_id": "6e76879a-d31d-4cc1-b6d2-076e85bbad33",
                     "tray_type": "all_pbs"
                     },
            "metadata": {
                "correlation_id": None,
                "timestamp": "2024-12-03 12:35:22",
                "trace_id": trace_id
             }
        }

def generate_orchestrator_command_pbs_start_slot_scan(correlation_id,trace_id):
    return {
        "id": str(uuid.uuid4()),
    }

# timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

def generate_orachestrator_scan_pbs_aoi_scan_started(correlation_id,trace_id):

    return {
          "id": str(uuid.uuid4()),
          "originator": "Garuda_Scanner",
          "version": 1,
          "metadata": {
            "correlation_id": None,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "trace_id": trace_id
          },
          "type": "pbs_aoi_scan_started",
          "data": {
            "pbs_main_scan_session_id": "0a44456a-fbe7-402f-83cc-d0b09bf8629b",
            "slot_id": "4",
            "aoi_id": "1",
            "qi_check": "True",
            "capture_level": "100X",
            "scan_pattern": "snake",
            "scan_pattern_first_direction": "left",
            "scan_pattern_second_direction": "down",
            "ppm": "8.05",
            "fov_height_px": "1080",
            "fov_width_px": "1440",
            "estimated_total_fov_count": "1628",
            "estimated_number_of_x_images": "37",
            "estimated_number_of_y_images": "44",
            "aoi_dir_path": "/imgarc/nila/data/shared/0a44456a-fbe7-402f-83cc-d0b09bf8629b/slot_4/aoi_1",
            "illumination_profile": "/imgarc/nila/data/shared/illumination-profile/illumination_profile_raw_150.pkl",
            "overlap_x_px": "132",
            "overlap_y_px": "134"
          }
        }

def generate_orchestrator_reconstruct_pbs_fov(correlation_id,trace_id):

    return {
      "id": str(uuid.uuid4()),
      "originator": "Garuda_Scanner",
      "version": 1,
      "metadata": {
        "correlation_id": None,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"),
        "trace_id": trace_id
      },
      "type": "reconstruct_pbs_fov",
      "data": {
        "scan_session_id": None,
        "slot_id": "4",
        "aoi_id": "1",
        "fov_data": {
          "path": "/imgarc/nila/data/shared/0a44456a-fbe7-402f-83cc-d0b09bf8629b/slot_4/aoi_1/raw_frames/36_raw_frame_folder",
          "best_sharp_idx": "5",
          "ppm": "8.05",
          "base_file_name": "36-scan_fov_ca_1_2f28a5ab-4965-47dc-bdb6-a4a85345162b"
        },
        "current_fov_count": "36",
        "estimated_total_fov_count": "1628.0",
        "elapsed_time": "11.953068733215332"
      }
}



