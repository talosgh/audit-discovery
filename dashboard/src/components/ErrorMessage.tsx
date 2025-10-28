import type { Component } from 'solid-js';

interface ErrorMessageProps {
  message: string;
  onRetry?: () => void;
}

const ErrorMessage: Component<ErrorMessageProps> = (props) => {
  return (
    <div class="error-box" role="alert">
      <strong>We hit a snag.</strong>
      <span>{props.message}</span>
      {props.onRetry && (
        <button type="button" class="refresh-button" onClick={props.onRetry}>
          Try again
        </button>
      )}
    </div>
  );
};

export default ErrorMessage;
