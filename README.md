# polybot on Render (free web service, no card) - PREPARED, NOT DEPLOYED
PAPER ONLY. Same app as ../polybot-space (HF), plus Render keep-alive (self-ping RENDER_EXTERNAL_URL every 10 min).
Refresh app/ from the live box bot: ./prepare.sh
Deploy (after Shalev signs up at https://dashboard.render.com/register with GitHub, no card):
  1. push this dir to a PRIVATE GitHub repo (Shalevfa/polybot-render), 2. New > Blueprint > pick repo,
  3. set HF_TOKEN + PUSH_SECRET, 4. run in MODE=shadow, compare with box, 5. MODE=primary + stop box bot.
Backup pinger: https://cron-job.org (free, email signup) -> GET https://<service>.onrender.com/health every 5 min.
