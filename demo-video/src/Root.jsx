import React from 'react';
import { Composition } from 'remotion';
import { DemoVideo, TOTAL_FRAMES } from './DemoVideo';

export const RemotionRoot = () => (
  <Composition
    id="DemoVideo"
    component={DemoVideo}
    durationInFrames={TOTAL_FRAMES}
    fps={30}
    width={1280}
    height={720}
  />
);
