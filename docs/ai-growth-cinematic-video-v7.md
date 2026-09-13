# NVIDIA server-room background — V7

Owner direction: match the background to the video's subject. For the NVIDIA
walkthrough, use a server rack room rather than general architectural imagery.

`cinematic_v3` is the navigation renderer default. The reviewed NVIDIA navigation
campaign selects `walnut-nvidia-server-room-v1.png`; unrelated campaigns retain
the original fallback rather than receiving NVIDIA imagery automatically.
V1 and V2 remain reproducible with the original atrium asset.

The background keeps V6's 45% brightness, slow motion and dark footer. The tight
caption boxes, real navigation, narration, logo, timing and existing numerical
masks are retained. The data center is illustrative, not an actual NVIDIA site.

Local output: `backend/artifacts/product-ad-v7/walnut-nvda-cinematic-v7.mp4`.
Source footage and continuous audio: the same V4 manifest used by V5 and V6;
every source file is checked against its recorded SHA-256 before rendering.

## Image provenance

Generated with built-in ImageGen, without input images or copied commercial assets.
Saved asset: `backend/app/assets/growth/walnut-nvidia-server-room-v1.png`.

Exact prompt:

> Use case: ads-marketing. Asset type: cinematic background plate for a vertical 9:16 Walnut Markets NVIDIA stock-research product video. Primary request: a photorealistic high-end GPU server rack room, illustrating NVIDIA's AI infrastructure topic. Scene: a real-looking modern data center aisle with tall black server cabinets on both sides, subtle tiny mint-green status LEDs, restrained overhead cool light and deep navy-black shadows. Composition: portrait 9:16, symmetrical perspective down the aisle; server cabinet texture visible especially along left and right edges and upper quarter; uncluttered darker center to sit behind a large product screen overlay. Camera: sophisticated architectural commercial photography, crisp authentic rack detail, subtle depth and natural reflections. Lighting: intentionally low-key and dim, cinematic but readable silhouettes; restrained mint-green highlights matching Walnut branding, no broad bright light source, no blown-out white panels. Constraints: background only, no people, no typography, no logos, no NVIDIA wordmark, no charts, no UI, no watermark. This is an illustrative generic data center, not a claim to depict an actual NVIDIA facility. High-resolution portrait image.
