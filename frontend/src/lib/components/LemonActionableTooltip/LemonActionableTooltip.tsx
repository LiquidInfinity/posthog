import React from 'react'
import { Placement } from '@floating-ui/react-dom-interactions'
import { Popup } from 'lib/components/Popup/Popup'
import { IconOpenInNew } from 'lib/components/icons'
import { IconClose, IconChevronLeft, IconChevronRight } from 'lib/components/icons'
import { LemonButton } from '@posthog/lemon-ui'
import './LemonActionableTooltip.scss'

export type LemonActionableTooltipProps = {
    title?: string
    text: string
    placement: Placement
    step: number
    maxSteps: number
    visible: boolean
    close: () => void
    element?: HTMLElement
    next?: () => void
    previous?: () => void
    buttons?: { label: string; url?: string; action?: () => void }[]
    icon?: JSX.Element
}

export const LemonActionableTooltip = ({
    title,
    text,
    element,
    placement,
    visible,
    close,
    previous,
    next,
    step,
    maxSteps,
    buttons,
    icon,
}: LemonActionableTooltipProps): JSX.Element | null => {
    const actionButtons = buttons?.filter((button) => button.action)
    const urlButtons = buttons?.filter((button) => button.url)
    return (
        <Popup
            visible={visible}
            referenceElement={element}
            placement={placement}
            overlay={
                <div className="LemonActionableTooltip">
                    <div className="LemonActionableTooltip__header">
                        {icon && <div className="LemonActionableTooltip__icon">{icon}</div>}
                        <div className="LemonActionableTooltip__title">{title ?? ''}</div>
                        <LemonButton size="small" status="stealth" onClick={close}>
                            <IconClose />
                        </LemonButton>
                    </div>
                    <div className="LemonActionableTooltip__body">
                        <div>{text}</div>
                        {urlButtons && (
                            <div className="LemonActionableTooltip__url_buttons">
                                {urlButtons.map((button, index) => (
                                    <LemonButton
                                        key={index}
                                        type="secondary"
                                        icon={<IconOpenInNew />}
                                        onClick={() => window.open(button.url, '_noblank')}
                                        className="max-w-full"
                                    >
                                        {button.label}
                                    </LemonButton>
                                ))}
                            </div>
                        )}
                    </div>
                    <div className="LemonActionableTooltip__footer">
                        {maxSteps > 1 && (
                            <div className="LemonActionableTooltip__navigation">
                                <LemonButton
                                    className="LemonActionableTooltip__navigation--left"
                                    onClick={previous}
                                    disabled={step === 0}
                                    size="small"
                                    status="stealth"
                                    icon={<IconChevronLeft />}
                                />
                                <div>
                                    Tip {step + 1} of {maxSteps}
                                </div>
                                <LemonButton
                                    className="LemonActionableTooltip__navigation--right"
                                    onClick={next}
                                    disabled={step === maxSteps - 1}
                                    size="small"
                                    status="stealth"
                                    icon={<IconChevronRight />}
                                />
                            </div>
                        )}
                        {actionButtons && (
                            <div className="LemonActionableTooltip__action_buttons">
                                {actionButtons.map((button, index) => {
                                    return (
                                        <LemonButton key={index} type="secondary" onClick={button.action}>
                                            {button.label}
                                        </LemonButton>
                                    )
                                })}
                            </div>
                        )}
                    </div>
                </div>
            }
            actionable
            showArrow
        />
    )
}
