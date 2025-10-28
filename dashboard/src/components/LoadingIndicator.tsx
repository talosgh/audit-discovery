import type { Component } from 'solid-js';

interface LoadingIndicatorProps {
  message?: string;
}

const LoadingIndicator: Component<LoadingIndicatorProps> = (props) => {
  return (
    <div class="loader" role="status" aria-live="polite">
      <span />
      <span />
      <span />
      <div>{props.message ?? 'Loading…'}</div>
    </div>
  );
};

export default LoadingIndicator;
