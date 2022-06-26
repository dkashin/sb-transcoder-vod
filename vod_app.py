#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os, re, time, shutil, threading, logging, shlex, json, datetime, select

from subprocess import PIPE, STDOUT, Popen, check_output, CalledProcessError

# Global settings

# App root path
path_root = os.path.abspath(os.path.dirname(__file__))
# Binary path
path_bin = os.path.join(path_root, 'bin')
# Source path
path_source = os.path.join(path_root, 'source')
# Encoder path (temporary)
path_encode = os.path.join(path_root, 'encode')
# Output path
path_target = os.path.join(path_root, 'target')
# Encoder binary path
path_bin_enc = os.path.join(path_bin, 'ffmpeg')
# Media check binary path
path_bin_chk = os.path.join(path_bin, 'ffprobe')
# Max active parallel jobs allowed
allowed_jobs = 4
# Delete job after encoding
job_delete_encoded = True
# Encoding threads limit per job, 0 = all threads
ff_threads = 0
# Encoding log name
log_encoder = os.path.join(path_root, 'encoder.log')
# Encoding error(s) log name
log_errors = os.path.join(path_root, 'errors.log')
# Log level
log_level = logging.DEBUG
# Source media format(s) list
ext_filter = [ 'avi', 'mkv', 'mpg', 'mpeg', 'vob', 'ts', 'mp4', 'wmv', 'mov', 'm4v' ]
# Target media extension
ext_target = 'mp4'
# Target media format
encoder_target_format = 'mp4'

# Encoder settings

# Use automatic SD/HD encoding settings
encoder_auto_quality = True

# Video codec
encoder_vcodec = 'libx264'
# Video bitrate (default)
encoder_vbitrate = '2500k'
# Video bitrate SD
encoder_vbitrate_sd = '1500k'
# Video bitrate HD
encoder_vbitrate_hd = '5000k'
# Video preset:
# ultrafast, superfast, veryfast, faster, fast, medium, slow, slower, veryslow
encoder_preset = 'fast'
# Video profile: baseline, main, high
encoder_profile = 'main'
# Video GOP size
encoder_gop = 200
# Video min keyframe interval
encoder_keyint_min = 50

# Audio codec
encoder_acodec = 'libfdk_aac'
# Audio bitrate (default)
encoder_abitrate = '96k'
# Audio bitrate SD
encoder_abitrate_sd = '96k'
# Audio bitrate HD
encoder_abitrate_hd = '256k'
# Audio channels output
encoder_achannels = 2

# Video deinterlace filter (interpolate fields)
encoder_deint = False


global active_jobs
active_jobs = 0

symbols = (
  'абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ',
  'abvgdeejzijklmnoprstufhzcss_y_euaABVGDEEJZIJKLMNOPRSTUFHZCSS_Y_EUA'
)

tr = dict( [ (ord(a), ord(b)) for (a, b) in zip(*symbols) ] )

rep_char = [ '____','___','__',' ','...','..','.','(',')',',','-','&' ]

del_char = [ '.avi','576p','1280x544','720p','1080p','BluRay','DTS','DVBrip','DVB','DD5.1','P1','P2','MVO','448','5-1', 'WEB-DLRip','DVDScr','HDDVDRip','tvrip','SATrip','SATRip','HDTVRip','BDRip','BDrip','Bluray','DVDRip','DVDrip','HDRip','DVDRIP','TVRip','SAT','CD','BR','R1','R5','HANSMER','by.Seven','HQCLUB','HQ','ivanes','ViDEO','srt','SRT', 'aac','AAC','ac3','AC3','.AVC','AVC','.avc','x264','H264','h264','XviD','Xvid','xvid','DivX','divx','Divx','.rus','.eng','RUS','ENG','Rus','Eng','Subs','Sub','.sub','.subs','SUBS','Dub','DUB','ELEKTRI4KA','ru-en-enCom','rus-eng','by.minik','ь','ъ','+','2x','tRu','DUAL','[rutracker.org]','[torrents.ru]','[Youtracker]','KAMO','th0r','[',']','PuzKarapuz','NNM-CLUB','[apreder]','HELLYWOOD','BestVideo','ShareReactor.ru','soperedi','\'','«','»','......','.....','....','...','..','`','!'
	]


def Logger(log_name = None, log_file = None):
  logger = logging.getLogger(log_name)
  logger.setLevel(log_level)
  handler = logging.FileHandler(log_file)
  formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
  handler.setFormatter(formatter)
  logger.addHandler(handler)
  return logger


logger_enc = Logger(log_name = 'encoder', log_file = log_encoder)
logger_err = Logger(log_name = 'error', log_file = log_errors)


def SystemStartupCheck():
  dirs = [ path_bin, path_source, path_encode, path_target ]
  for d in dirs:
    try:
      os.makedirs(d)
    except OSError as e:
      if not 'File exist' in str(e):
        logger_enc.error(f'[System] InitDirTree Error: {e}')
        logger_err.error(f'[System] InitDirTree Error: {e}')
    except:
      logger_enc.error('[System] InitDirTree exception error')
      logger_err.error('[System] InitDirTree exception error')
  bins = [ path_bin_enc, path_bin_chk ]
  for b in bins:
    if not os.path.exists(b):
      logger_enc.error(f'[System] Startup check error: {b} is not found')
      logger_err.error(f'[System] Startup check error: {b} is not found')
      print(f'[System] Startup check error: {b} is not found')
      return False
  logger_enc.info('[System] Startup check OK')
  print('[System] Startup check OK')
  return True


class Encoder(threading.Thread):

  def __init__(self, logger, file_enc_norm_abs, src_media_info, cmd):
    threading.Thread.__init__(self)
    self.logger = logger
    self.src_media_info = src_media_info
    self.file_enc_norm_abs = file_enc_norm_abs
    self.file_enc_norm = file_enc_norm_abs.split('/')[-1:]
    self.cmd = cmd

  def run(self):
    global active_jobs
    result = { 'complete': False, 'msg': 'Internal error' }
    try:
      p = Popen(shlex.split(self.cmd), stdout = PIPE, stderr = STDOUT, close_fds = True, universal_newlines = True)
      s = select.poll()
      s.register(p.stdout, select.POLLIN)
      src_media_info_format = self.src_media_info.get('format')
      src_duration_delta = src_media_info_format.get('duration')
      enc_progress_last = 0
      active_jobs = active_jobs + 1
      while True:
        if s.poll(1):
          try:
            stat = (p.stdout.readline()).split('\n')[0]
          except UnicodeDecodeError:
            stat = ''
          if stat or p.poll() == None:
            stat_last = stat
            if all(st in stat for st in [ 'time', 'bitrate' ]):
              stat_dict = dict(re.findall(r"\b(\w+)\s*=\s*([^=]*)(?=\s+\w+\s*=|$)", stat))
              stat_time = stat_dict.get('time').split('.')[0]
              stat_time = datetime.datetime.strptime(stat_time, '%H:%M:%S')
              stat_time_delta = datetime.timedelta(hours = stat_time.hour, minutes = stat_time.minute, seconds = stat_time.second)
              stat_time_left = (src_duration_delta - stat_time_delta)
              stat_time_left_sec = int(stat_time_left.total_seconds())
              src_duration_delta_sec = int(src_duration_delta.total_seconds())
              enc_progress = int(100 - (stat_time_left_sec / src_duration_delta_sec * 100 ))
              if ((enc_progress - enc_progress_last >= 15) or (enc_progress == 100)) and enc_progress != enc_progress_last:
                enc_progress_last = enc_progress
                stat_line = f"{self.file_enc_norm} Complete: {enc_progress}% | Bitrate: {stat_dict.get('bitrate')} | Encoding speed {stat_dict.get('speed')}"
                print(f'[VODEncode] {stat_line}')
                self.logger.info(f'[VODEncode] {stat_line}')
          else:
            break
      if p.returncode == 1:
        result = { 'msg': f'{stat_last}' }
      elif p.returncode == 255:
        result = { 'msg': 'User interrupted' }
      else:
        result = { 'complete': True, 'msg': 'OK' }
    except OSError as e:
      result = { 'msg': f'OSError ({e})' }
    except ValueError as e:
      result = { 'msg': f'ValueError ({e})' }
    if result.get('complete'):
      if job_delete_encoded:
        try:
          os.remove(self.file_enc_norm_abs)
          print(f'[VODEncode] {self.file_enc_norm} Source wiped')
          logger_enc.info(f'[VODEncode] {self.file_enc_norm} Source wiped')
        except:
          print(f'[VODEncode] {self.file_enc_norm} Source wipe error')
          logger_enc.error(f'[VODEncode] {self.file_enc_norm} Source wipe error')
      print(f'[VODEncode] {self.file_enc_norm} Encoding complete')
      logger_enc.info(f'[VODEncode] {self.file_enc_norm} Encoding complete')
    else:
      print(f"[VODEncode] {self.file_enc_norm} Encoding error ({result.get('msg')})")
      logger_enc.error(f"[VODEncode] {self.file_enc_norm} Encoding error ({result.get('msg')})")
      logger_err.error(f"[VODEncode] {self.file_enc_norm} Encoding error ({result.get('msg')})")
    active_jobs = active_jobs - 1


def VODRename(file_src):
  file_ext = file_src.split('.')[-1:][0]
  fn_corr = file_src[:-len(file_ext)-1]
  while any(char in fn_corr for char in del_char):
    for char in del_char:
      fn_corr = fn_corr.replace(char, '')
  while any(char in fn_corr for char in rep_char):
    for char in rep_char:
      fn_corr = fn_corr.replace(char, '_')
  if fn_corr.endswith('_'):
    fn_corr = fn_corr[:-1].replace(char, '')
  file_src_norm = f"{fn_corr.translate(tr)}.{file_ext}"
  print(f'[VODRename] {file_src} --> {file_src_norm}')
  logger_enc.info(f'[VODRename] {file_src} --> {file_src_norm}')
  return file_src_norm


# Media info
def MediaInfo(media = None, media_fn = None, timeout = 60):
  media_data = None
  src_height = None
  check_timeout = f'timeout {timeout} '
  check_format = ' -of json'
  check_duration = ' -analyzeduration 5000000' # 3 sec
  check_options = ' -hide_banner -v quiet -show_error -select_streams v -show_entries format=format_name,bit_rate,duration,size:stream=codec_type,width,height,codec_name,r_frame_rate,field_order '
  try:
    check_cmd = check_timeout + path_bin_chk + check_format + check_duration + check_options + media
    out = check_output(shlex.split(check_cmd), close_fds = True).decode()
    try:
      mi_json_data = json.loads(out)
      if 'streams' in list(mi_json_data.keys()):
        media_data = mi_json_data
      else:
        msg = f'No media data ({out})'
    except ValueError as e:
      msg = f'Media data ValueError ({e})'
    except:
      msg = f'Media data error exception'
  except CalledProcessError as e:
    try:
      output = json.loads(e.output)
      msg = f"CalledProcessError error ({output['error']['string']})"
    except:
      msg = f'CalledProcessError error ({e})'
  except:
    msg = f'Exception error'
  if media_data:
    scr_format = media_data.get('format')
    src_format_name = scr_format.get('format_name')
    src_duration = datetime.timedelta(seconds = int(float(scr_format.get('duration'))))
    media_data['format']['duration'] = src_duration
    src_size = int(scr_format.get('size'))
    if src_size > 1000000:
      src_size = f'{int(src_size/1024/1024)}M'
    else:
      src_size = f'{int(src_size/1024)}k'
    src_bitrate = scr_format.get('bit_rate')
    scr_video = media_data.get('streams')[0]
    src_codec_name = scr_video.get('codec_name')
    src_width = scr_video.get('width')
    src_height = scr_video.get('height')
    src_field_order = scr_video.get('field_order')
    src_fps = scr_video.get('r_frame_rate')
    mi = f'[MediaInfo] [{media_fn}]\n \
    Format: {src_format_name} | Codec: {src_codec_name}\n \
    Duration {src_duration} | Size: {src_size}\n \
    Frame: {src_width}x{src_height} | Fields: {src_field_order}\n \
    FPS: {src_fps}'
    print(mi)
    logger_enc.info(mi)
  else:
    print(f'[MediaInfo] [{media_fn}] {msg}')
    logger_enc.error(f'[MediaInfo] [{media_fn}] {msg}')
    logger_err.error(f'[MediaInfo] [{media_fn}] {msg}')
  return media_data, src_height


def VODEncode(file_src_abs, file_src):
    try:
      file_src_norm = VODRename(file_src)
      file_enc_norm_abs = os.path.join(path_encode, file_src_norm)
      os.rename(file_src_abs, file_enc_norm_abs)
      file_out_norm_ext = f"{file_src_norm.split('.')[:-1][0]}.{ext_target}"
      file_out_norm_abs = os.path.join(path_target, file_out_norm_ext)
    except:
      print(f'[VODRename] [{file_src}] Encoding error')
      logger_enc.error(f'[VODRename] [{file_src}] Encoding error')
      logger_err.error(f'[VODRename] [{file_src}] Encoding error')
      return None
    src_media_info, src_height = MediaInfo(media = file_enc_norm_abs, media_fn = file_src)
    if not src_media_info:
      print(f'[MediaInfo] [{file_src}] Encoding error')
      logger_enc.error(f'[MediaInfo] [{file_src}] Encoding error')
      logger_err.error(f'[MediaInfo] [{file_src}] Encoding error')
      return None
    if encoder_auto_quality:
      print(f'[VODEncode] [{file_src}] Encoder auto quality enabled')
      logger_enc.info(f'[VODEncode] [{file_src}] Encoder auto quality enabled')
      if src_height < 700:
        src_type = 'sd'
        encoder_vbitrate = encoder_vbitrate_sd
        encoder_abitrate = encoder_abitrate_sd
        print(f'[VODEncode] [{file_src}] Source has SD quality')
        logger_enc.info(f'[VODEncode] [{file_src}] Source has SD quality')
      else:
        src_type = 'hd'
        encoder_vbitrate = encoder_vbitrate_hd
        encoder_abitrate = encoder_abitrate_hd
        print(f'[VODEncode] [{file_src}] Source has HD quality')
        logger_enc.info(f'[VODEncode] [{file_src}] Source has HD quality')
    ff_cmd = f"{path_bin_enc} \
      -y -hide_banner \
      -loglevel repeat \
      -threads {ff_threads} \
      -i {file_enc_norm_abs} \
      -c:v {encoder_vcodec} \
      -preset:v {encoder_preset} \
      -profile:v {encoder_profile} \
      -b:v {encoder_vbitrate} \
      -pix_fmt yuv420p \
      -g {encoder_gop} \
      -keyint_min {encoder_keyint_min} \
      -force_key_frames \'expr:gte(t,n_forced*2)\' \
      -strict -2 \
      -c:a {encoder_acodec} \
      -b:a {encoder_abitrate} \
      -ac {encoder_achannels} \
      { '-filter:v yadif=0' if encoder_deint else '' } \
      -f {encoder_target_format} \
      {file_out_norm_abs}"
    encoder = Encoder(logger_enc, file_enc_norm_abs, src_media_info, ff_cmd)
    encoder.start()
    print(f'[VODEncode] [{file_src_norm}] Encoding started')
    logger_enc.info(f'[VODEncode] [{file_src_norm}] Encoding started')
    print(f'[System] Active jobs: {active_jobs} / {allowed_jobs}')
    logger_enc.info(f'[System] Active jobs: {active_jobs} / {allowed_jobs}')


if SystemStartupCheck():
  print(f'[System] Checking source dir [{path_source}]')
  logger_enc.info(f'[System] Checking source dir [{path_source}]')
  while True:
    for rt, d, files in os.walk(path_source):
      for file in files:
        if file.split('.')[-1:][0] in ext_filter:
          try:
            # Waiting for file upload completion (stable size)
            fs1 = os.path.getsize(os.path.join(rt, file))
            time.sleep(5)
            fs2 = os.path.getsize(os.path.join(rt, file))
            if fs1 == fs2:
              print(f'[System] Got new source [{file}]')
              logger_enc.info(f'[System] Got new source [{file}]')
              file_src_abs = os.path.abspath(os.path.join(rt, file))
              VODEncode(file_src_abs, file)
              while active_jobs >= allowed_jobs:
                time.sleep(1)
          except FileNotFoundError as e:
            logger_enc.error(f'[System] FileNotFoundError ({e})')
            logger_err.error(f'[System] FileNotFoundError ({e})')
          except OSError as e:
            logger_enc.error(f'[System] Source OSError ({e})')
            logger_err.error(f'[System] Source OSError ({e})')
          except:
            logger_enc.error('[System] Source exception error')
            logger_err.error('[System] Source exception error')
    time.sleep(3)
else:
  logger_enc.error(f'[System] Exit')
  print(f'[System] Exit')

