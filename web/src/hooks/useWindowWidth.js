import { useState, useEffect } from 'react';

export function useWindowWidth() {
  const [width, setWidth] = useState(window.innerWidth);
  useEffect(() => {
    const handler = () => setWidth(window.innerWidth);
    window.addEventListener('resize', handler);
    return () => window.removeEventListener('resize', handler);
  }, []);
  return width;
}

export function useBreakpoint() {
  const w = useWindowWidth();
  return {
    isMobile:  w < 640,
    isTablet:  w >= 640 && w < 1024,
    isDesktop: w >= 1024,
    width: w,
  };
}
