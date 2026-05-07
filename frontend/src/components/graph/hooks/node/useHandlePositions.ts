import { useCallback, useLayoutEffect, useRef, useState } from "react";

// Measures DOM row positions for handle placement. Pass deps to re-measure on changes.
export function useHandlePositions(deps: unknown[] = []) {
  const rowRefs = useRef<Map<string, HTMLDivElement>>(new Map());
  const [rowPositions, setRowPositions] = useState<Record<string, number>>({});

  const measure = useCallback(() => {
    const positions: Record<string, number> = {};
    for (const [key, el] of rowRefs.current.entries()) {
      const rowRect = el.getBoundingClientRect();
      const container = el.closest("[data-node-card]");
      const containerRect = container?.getBoundingClientRect();
      if (container instanceof HTMLElement && containerRect) {
        const scaleY = container.offsetHeight
          ? containerRect.height / container.offsetHeight
          : 1;
        const screenCenterY = rowRect.top - containerRect.top + rowRect.height / 2;
        positions[key] = screenCenterY / (scaleY || 1);
      } else {
        positions[key] = el.offsetTop + el.offsetHeight / 2;
      }
    }
    setRowPositions(prev => {
      const keys = Object.keys(positions);
      if (
        keys.length === Object.keys(prev).length &&
        keys.every(k => Math.abs((prev[k] ?? 0) - positions[k]) < 1)
      ) {
        return prev; // no change
      }
      return positions;
    });
  }, []);

  const setRowRef = useCallback(
    (key: string) => (el: HTMLDivElement | null) => {
      if (el) {
        rowRefs.current.set(key, el);
        requestAnimationFrame(measure);
      } else {
        rowRefs.current.delete(key);
        requestAnimationFrame(measure);
      }
    },
    [measure]
  );

  useLayoutEffect(() => {
    measure();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  useLayoutEffect(() => {
    if (typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(() => {
      requestAnimationFrame(measure);
    });
    const observed = new Set<Element>();
    for (const el of rowRefs.current.values()) {
      observed.add(el);
      const nodeCard = el.closest("[data-node-card]");
      if (nodeCard) observed.add(nodeCard);
      if (el.offsetParent instanceof Element) observed.add(el.offsetParent);
    }
    observed.forEach(el => observer.observe(el));
    return () => observer.disconnect();
  }, [measure, rowPositions]);

  return { setRowRef, rowPositions };
}
