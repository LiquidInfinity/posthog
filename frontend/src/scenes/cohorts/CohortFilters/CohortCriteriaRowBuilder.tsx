import './CohortCriteriaRowBuilder.scss'
import React from 'react'
import { BehavioralFilterType, CohortFieldProps, Field, FilterType } from 'scenes/cohorts/CohortFilters/types'
import { renderField, ROWS } from 'scenes/cohorts/CohortFilters/constants'
import { Col, Divider, Row } from 'antd'
import { LemonButton } from 'lib/components/LemonButton'
import { IconCopy, IconDelete } from 'lib/components/icons'
import { AnyCohortCriteriaType, BehavioralEventType, FilterLogicalOperator } from '~/types'
import clsx from 'clsx'
import { Field as KeaField } from 'kea-forms'
import { AlertMessage } from 'lib/components/AlertMessage'
import { useActions } from 'kea'
import { cleanCriteria } from 'scenes/cohorts/cohortUtils'
import { cohortEditLogic } from 'scenes/cohorts/cohortEditLogic'
import { CohortLogicProps } from 'scenes/cohorts/cohortLogic'

export interface CohortCriteriaRowBuilderProps {
    id: CohortLogicProps['id']
    criteria: AnyCohortCriteriaType
    type: BehavioralFilterType
    groupIndex: number
    index: number
    logicalOperator: FilterLogicalOperator
    hideDeleteIcon?: boolean
    onChangeType?: (nextType: BehavioralFilterType) => void
}

export function CohortCriteriaRowBuilder({
    id,
    type,
    groupIndex,
    index,
    logicalOperator,
    criteria,
    hideDeleteIcon = false,
    onChangeType,
}: CohortCriteriaRowBuilderProps): JSX.Element {
    const { setCriteria, duplicateFilter, removeFilter } = useActions(cohortEditLogic({ id }))
    const rowShape = ROWS[type]

    const renderFieldComponent = (_field: Field, i: number): JSX.Element => {
        return (
            <Col key={_field.fieldKey ?? i}>
                {renderField[_field.type]({
                    fieldKey: _field.fieldKey,
                    criteria,
                    ...(_field.type === FilterType.Text ? { value: _field.defaultValue } : {}),
                    ...(!!_field.groupTypeFieldKey ? { groupTypeFieldKey: _field.groupTypeFieldKey } : {}),
                    onChange: (newCriteria) => setCriteria(newCriteria, groupIndex, index),
                } as CohortFieldProps)}
            </Col>
        )
    }

    return (
        <div className="CohortCriteriaRow">
            {index !== 0 && <LogicalRowDivider logicalOperator={logicalOperator} />}
            <KeaField
                name="id"
                template={({ error, kids }) => {
                    return (
                        <>
                            <div
                                className={clsx(
                                    'CohortCriteriaRow__Criteria',
                                    error && `CohortCriteriaRow__Criteria--error`
                                )}
                            >
                                {kids}
                                {error && (
                                    <AlertMessage className="my-2" type="error">
                                        {error}
                                    </AlertMessage>
                                )}
                            </div>
                        </>
                    )
                }}
            >
                <>
                    <Row align="middle" wrap={false} className="mb-1">
                        <KeaField
                            name="value"
                            template={({ error, kids }) => {
                                return (
                                    <>
                                        <div
                                            className={clsx(
                                                'CohortCriteriaRow__Criteria__Field',
                                                error && `CohortCriteriaRow__Criteria__Field--error`
                                            )}
                                        >
                                            {kids}
                                        </div>
                                    </>
                                )
                            }}
                        >
                            <>
                                <Col>
                                    {renderField[FilterType.Behavioral]({
                                        fieldKey: 'value',
                                        criteria,
                                        onChange: (newCriteria) => {
                                            setCriteria(cleanCriteria(newCriteria, true), groupIndex, index)
                                            onChangeType?.(newCriteria['value'] ?? BehavioralEventType.PerformEvent)
                                        },
                                    })}
                                </Col>
                            </>
                        </KeaField>
                        <div className="CohortCriteriaRow__inline-divider" />
                        <LemonButton
                            icon={<IconCopy />}
                            status="primary-alt"
                            onClick={() => duplicateFilter(groupIndex, index)}
                        />
                        {!hideDeleteIcon && (
                            <LemonButton
                                icon={<IconDelete />}
                                status="primary-alt"
                                onClick={() => removeFilter(groupIndex, index)}
                            />
                        )}
                    </Row>
                    <div style={{ display: 'flex' }}>
                        <Col>
                            <span className="CohortCriteriaRow__Criteria__arrow">&#8627;</span>
                        </Col>
                        <Col>
                            <Row align="middle">
                                {rowShape.fields.map((field, i) => {
                                    return (
                                        !field.hide &&
                                        (field.fieldKey ? (
                                            <KeaField
                                                key={i}
                                                name={field.fieldKey}
                                                template={({ error, kids }) => {
                                                    return (
                                                        <>
                                                            <div
                                                                className={clsx(
                                                                    'CohortCriteriaRow__Criteria__Field',
                                                                    error && `CohortCriteriaRow__Criteria__Field--error`
                                                                )}
                                                            >
                                                                {kids}
                                                            </div>
                                                        </>
                                                    )
                                                }}
                                            >
                                                <>{renderFieldComponent(field, i)}</>
                                            </KeaField>
                                        ) : (
                                            <div key={i} className="CohortCriteriaRow__Criteria__Field">
                                                {renderFieldComponent(field, i)}
                                            </div>
                                        ))
                                    )
                                })}
                            </Row>
                        </Col>
                    </div>
                </>
            </KeaField>
        </div>
    )
}

export interface LogicalRowDividerProps {
    logicalOperator: FilterLogicalOperator
}

export function LogicalRowDivider({ logicalOperator }: LogicalRowDividerProps): JSX.Element {
    return (
        <Divider className="logical-row-divider" orientation="left">
            {logicalOperator}
        </Divider>
    )
}
