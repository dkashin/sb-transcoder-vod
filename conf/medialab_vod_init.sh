ln -sf /opt/medialab_vod/conf/medialab_vod.service /etc/systemd/system/multi-user.target.wants
sudo systemctl daemon-reload
sudo systemctl restart medialab_vod
sudo systemctl status medialab_vod --no-pager
